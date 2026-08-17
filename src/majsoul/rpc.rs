use anyhow::{Context, Result};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, warn};

const MS_HOST: &str = "https://game.maj-soul.com";

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub const FETCH_GAME_RECORD_METHOD: &str = ".lq.Lobby.fetchGameRecord";
pub const FETCH_GAME_LIVE_LIST_METHOD: &str = ".lq.Lobby.fetchGameLiveList";

pub fn build_fetch_game_record_request(uuid: &str, version: &str) -> Vec<u8> {
    requests::fetch_game_record(uuid, version)
}

pub fn build_fetch_game_live_list_request(filter_id: u32) -> Vec<u8> {
    requests::fetch_game_live_list(filter_id)
}

pub fn ensure_success_response(response: &[u8], operation: &str) -> Result<()> {
    if let Some(code) = MajsoulRpc::extract_error_code(response)
        && code != 0
    {
        return Err(anyhow::Error::new(ServerError { code })
            .context(format!("{operation} returned error {code}")));
    }
    Ok(())
}

/// Business-level error code carried inside a decoded server response.
/// Receiving one proves the socket and session answered, so callers must
/// not treat it as a transport failure (no reconnect will change it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerError {
    pub code: u64,
}

impl ServerError {
    /// Codes that mean the login/session went stale (ERR_OAUTH2_EXPIRED,
    /// ERR_OAUTH2_FAILED, ERR_ACC_NOT_LOGIN, ERR_TOKEN_NOT_EXIST,
    /// ERR_TOKEN_INVALID): unlike every other business code, a fresh login
    /// is exactly the cure for these.
    pub fn is_session_stale(&self) -> bool {
        matches!(self.code, 109 | 110 | 1004 | 1201 | 1202)
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error code {}", self.code)
    }
}

impl std::error::Error for ServerError {}

/// Simple protobuf Wrapper encoder/decoder
mod wrapper {
    use anyhow::Result;

    /// Wraps one request as `Wrapper { name, data }`.
    ///
    /// Field 2 is written even when the body is empty, because that is what the
    /// real client does: nine different empty-bodied methods across five
    /// captures — `loginSuccess`, `fetchInfo`, `fetchConnectionInfo` and the
    /// rest — all carry `12 00`, and not one omits the field.
    ///
    /// Protobuf treats the two encodings as the same message, so omitting it
    /// costs nothing and is never noticed locally. What it costs is on the wire:
    /// every session this deployment opens sends a `loginSuccess` two bytes
    /// shorter than any browser's, which is a length comparison away from being
    /// spotted.
    pub fn encode(name: &str, data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        // Field 1: name (string)
        buf.push(0x0a);
        encode_varint(&mut buf, name.len() as u64);
        buf.extend_from_slice(name.as_bytes());
        // Field 2: data (bytes), present even when empty
        buf.push(0x12);
        encode_varint(&mut buf, data.len() as u64);
        buf.extend_from_slice(data);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<(String, Vec<u8>)> {
        let mut pos = 0;
        let mut name = String::new();
        let mut data = Vec::new();

        while pos < buf.len() {
            let tag = buf[pos];
            pos += 1;
            let field_num = tag >> 3;
            let wire_type = tag & 0x07;

            if wire_type != 2 {
                if wire_type == 0 {
                    while pos < buf.len() && buf[pos] & 0x80 != 0 {
                        pos += 1;
                    }
                    pos += 1;
                }
                continue;
            }

            let (len, bytes_read) = decode_varint(&buf[pos..])?;
            pos += bytes_read;
            let end = pos + len as usize;
            if end > buf.len() {
                anyhow::bail!("Buffer overflow");
            }

            match field_num {
                1 => name = String::from_utf8_lossy(&buf[pos..end]).to_string(),
                2 => data = buf[pos..end].to_vec(),
                _ => {}
            }
            pos = end;
        }
        Ok((name, data))
    }

    pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize)> {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        let mut pos = 0;
        loop {
            if pos >= buf.len() {
                anyhow::bail!("Unexpected end in varint");
            }
            let byte = buf[pos];
            pos += 1;
            let part = (byte & 0x7f) as u64;
            // Reject non-canonical varints that overflow u64 instead of
            // panicking (debug) or silently wrapping the shift (release).
            if shift >= 64 || part > (u64::MAX >> shift) {
                anyhow::bail!("Varint overflows u64");
            }
            value |= part << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Ok((value, pos))
    }
}

pub mod requests {
    use super::wrapper::encode_varint;

    pub fn fetch_game_record(uuid: &str, version: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        // Field 1: game_uuid
        encode_string(&mut buf, 1, uuid);
        // Field 2: client_version_string
        encode_string(&mut buf, 2, version);
        buf
    }

    /// ReqGameLiveList { filter_id = 1 }
    pub fn fetch_game_live_list(filter_id: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, filter_id as u64);
        buf
    }

    /// Build ReqLogin for CN native login, byte-for-byte matching a captured
    /// real web-client login frame: reconnect=0, full android `device`,
    /// `client_version { resource, package }`, the exact currency_platforms
    /// list, type=0, `client_version_string` = `WebGL_2022-<code_version>`,
    /// and the server `tag`. Anything less is rejected by risk control (151).
    pub fn build_login_request(
        account: &str,
        password_hash: &str,
        random_key: &str,
        code_version: &str,
        package_version: &str,
        tag: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1: account (string)
        encode_string(&mut buf, 1, account);
        // Field 2: password (string, HMAC-SHA256 hex)
        encode_string(&mut buf, 2, password_hash);
        // Field 3: reconnect = false
        encode_varint_field(&mut buf, 3, 0);
        // Field 4: device — ClientDeviceInfo, one machine per account
        encode_device(&mut buf, account);
        // Field 5: random_key (string)
        encode_string(&mut buf, 5, random_key);
        // Field 6: client_version { resource, package }
        encode_client_version(&mut buf, code_version, package_version);
        // Field 7: gen_access_token (bool = true)
        encode_bool(&mut buf, 7, true);
        // Field 8: currency_platforms (repeated) — exact real-client set
        for platform in [1u64, 2, 5, 6, 8, 10, 11] {
            encode_varint_field(&mut buf, 8, platform);
        }
        // Field 9: type = 0
        encode_varint_field(&mut buf, 9, 0);
        // Field 11: client_version_string = WebGL_2022-<code_version>
        encode_string(&mut buf, 11, &format!("WebGL_2022-{code_version}"));
        // Field 12: tag (server, e.g. "cn")
        encode_string(&mut buf, 12, tag);

        buf
    }

    /// Build loginBeat request
    pub fn build_login_beat_request(contract: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_string(&mut buf, 1, contract);
        buf
    }

    fn encode_string(buf: &mut Vec<u8>, field: u32, value: &str) {
        let tag = (field << 3) | 2;
        encode_varint(buf, tag as u64);
        encode_varint(buf, value.len() as u64);
        buf.extend_from_slice(value.as_bytes());
    }

    fn encode_bool(buf: &mut Vec<u8>, field: u32, value: bool) {
        let tag = field << 3;
        encode_varint(buf, tag as u64);
        buf.push(if value { 1 } else { 0 });
    }

    fn encode_varint_field(buf: &mut Vec<u8>, field: u32, value: u64) {
        let tag = field << 3;
        encode_varint(buf, tag as u64);
        encode_varint(buf, value);
    }

    /// The Chrome versions a `mac` persona may claim. Kept in step with what the
    /// real client is seen sending; a version far from the fleet's is as much a
    /// marker as a wrong one.
    const CHROME_VERSIONS: [u32; 3] = [149, 150, 151];

    /// Real Mac screen sizes. The device reports the *viewport*, not the screen,
    /// so the height sent is this minus the browser's own furniture.
    const MAC_SCREENS: [(u64, u64); 6] = [
        (1512, 982),
        (1440, 900),
        (1728, 1117),
        (1280, 800),
        (1920, 1080),
        (2560, 1440),
    ];

    /// One machine, derived from the account name and nothing else.
    ///
    /// Two properties, and both matter. It is *stable*, because an account whose
    /// reported hardware changes between two logins is an anomaly no real person
    /// produces. And it is *distinct per account*, because the alternative is
    /// what this had before: every account in the pool reporting one identical
    /// machine — same user agent, same 923×830 viewport — from one address. That
    /// is the cheapest query the other side could possibly run to collect the
    /// whole fleet in one go, and no amount of care taken at registration time
    /// survives it.
    ///
    /// Derived rather than stored so it needs no migration and no file: the
    /// account name is already the identity everything else keys on.
    /// The GPU strings a Mac reports through WebGL, in the exact shape the
    /// browser renders them. Only the telemetry sends this, and only these
    /// spellings appear in the wild — inventing a format is more visible than
    /// repeating one.
    ///
    /// ponytail: mac-only, matching the personas. Add a row per platform once
    /// there is a capture of one.
    const MAC_GPUS: [&str; 7] = [
        "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)",
        "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Pro, Unspecified Version)",
        "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)",
        "ANGLE (Apple, ANGLE Metal Renderer: Apple M3, Unspecified Version)",
        "ANGLE (Apple, ANGLE Metal Renderer: Apple M4, Unspecified Version)",
        "ANGLE (Intel, ANGLE Metal Renderer: Intel(R) UHD Graphics 630, Unspecified Version)",
        "ANGLE (AMD, ANGLE Metal Renderer: AMD Radeon Pro 5500M, Unspecified Version)",
    ];

    /// One account's machine.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Persona {
        pub chrome: u32,
        /// What `device.f10`/`f11` report: the browser viewport, not the screen.
        pub viewport_width: u64,
        pub viewport_height: u64,
        /// What the telemetry reports as `device_gpu_name`.
        pub gpu: &'static str,
    }

    pub fn persona(account: &str) -> Persona {
        // FNV-1a. Not for security — it only has to spread account names evenly
        // over the tables and give the same answer every time.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in account.to_lowercase().bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let (width, height) = MAC_SCREENS[((hash >> 8) % MAC_SCREENS.len() as u64) as usize];
        Persona {
            chrome: CHROME_VERSIONS[(hash % CHROME_VERSIONS.len() as u64) as usize],
            // The width is the screen's; the height is the screen's less the
            // menu bar, tab strip and address bar — 220 to 259 pixels the page
            // never sees.
            viewport_width: width,
            viewport_height: height - 220 - ((hash >> 16) % 40),
            gpu: MAC_GPUS[((hash >> 24) % MAC_GPUS.len() as u64) as usize],
        }
    }

    /// Encode ClientDeviceInfo (field 4), field for field as a captured real web
    /// client sends it: `pc` / `pc` / `mac`, is_browser, `Chrome`, `web`, the
    /// viewport, the user agent, screen_type 1.
    ///
    /// What this replaced claimed to be an Android frame and was wrong in three
    /// checkable ways against five captures: it sent a field 4 (`android15`) the
    /// real client does not send at all, screen_type 2 where every capture has
    /// 1, and a fixed viewport shared by every account.
    fn encode_device(buf: &mut Vec<u8>, account: &str) {
        let Persona {
            chrome,
            viewport_width,
            viewport_height,
            ..
        } = persona(account);
        let mut inner = Vec::new();
        encode_string(&mut inner, 1, "pc"); // platform
        encode_string(&mut inner, 2, "pc"); // hardware
        encode_string(&mut inner, 3, "mac"); // os
        encode_bool(&mut inner, 5, true); // is_browser
        encode_string(&mut inner, 6, "Chrome"); // software
        encode_string(&mut inner, 7, "web"); // sale_platform
        encode_varint_field(&mut inner, 10, viewport_width); // screen_width
        encode_varint_field(&mut inner, 11, viewport_height); // screen_height
        encode_string(&mut inner, 12, &user_agent(chrome)); // user_agent
        encode_varint_field(&mut inner, 13, 1); // screen_type

        let tag = (4 << 3) | 2;
        encode_varint(buf, tag as u64);
        encode_varint(buf, inner.len() as u64);
        buf.extend(inner);
    }

    /// The user agent that goes with a Chrome version. It has to agree with
    /// everything else this deployment claims to be: a login saying `mac` and a
    /// user agent saying Windows is one comparison away from being spotted.
    pub fn user_agent(chrome: u32) -> String {
        format!(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/{chrome}.0.0.0 Safari/537.36"
        )
    }

    /// What a browser attaches to a call the game page makes to Mahjong Soul's
    /// HTTP API, taken from a captured request verbatim.
    ///
    /// All fifty API requests across five captures carry the same set. What this
    /// replaced sent a user agent and nothing else — a caller claiming to be
    /// Chrome while missing everything Chrome adds, which is a cheaper thing for
    /// the other side to notice than a user agent is to fake.
    ///
    /// `content-type` on a GET looks wrong and is not: the client sets it, so it
    /// is here. Copying what is sent beats copying what ought to be.
    pub fn browser_headers(chrome: u32) -> reqwest::header::HeaderMap {
        use reqwest::header::{HeaderMap, HeaderValue};
        let mut headers = HeaderMap::new();
        let mut put = |name: &'static str, value: String| {
            if let Ok(value) = HeaderValue::from_str(&value) {
                headers.insert(name, value);
            }
        };
        put("accept", "*/*".into());
        put("content-type", "text/html;charset=UTF-8".into());
        put("origin", super::MS_HOST.into());
        put("referer", format!("{}/", super::MS_HOST));
        put(
            "sec-ch-ua",
            format!(
                "\"Not=A?Brand\";v=\"99\", \"Google Chrome\";v=\"{chrome}\", \
                 \"Chromium\";v=\"{chrome}\""
            ),
        );
        put("sec-ch-ua-mobile", "?0".into());
        put("sec-ch-ua-platform", "\"macOS\"".into());
        headers
    }

    /// Encode ClientVersionInfo (field 6) { resource, package } — the client
    /// code version (e.g. `0.16.256`) and framework build (e.g. `4.0.45`),
    /// which differ from the resource version in version.json.
    fn encode_client_version(buf: &mut Vec<u8>, resource: &str, package: &str) {
        let mut inner = Vec::new();
        encode_string(&mut inner, 1, resource); // resource
        encode_string(&mut inner, 2, package); // package

        let tag = (6 << 3) | 2;
        encode_varint(buf, tag as u64);
        encode_varint(buf, inner.len() as u64);
        buf.extend(inner);
    }

    /// Build ReqRequestConnection for the route handshake (required before
    /// login). Verified against a captured real-client frame: type=1,
    /// route_id, unix-second timestamp, and field 6 = "Web". Sending type=3
    /// (or no field 6 at all) leaves the session in a state where login is
    /// rejected with server error 151.
    ///
    /// Capital W. Three 2026-08 captures carry `3203 576562` — `W` is `0x57` —
    /// and this sent `web`, which the gateway accepts but no client sends.
    /// The lower-case `web` in `encode_device` field 7 is a different field and
    /// really is lower case: the same registration carries both spellings.
    pub fn build_request_connection(route_id: &str, timestamp: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        // Field 2: type = 1
        encode_varint_field(&mut buf, 2, 1);
        // Field 3: route_id (string)
        encode_string(&mut buf, 3, route_id);
        // Field 4: timestamp (varint, unix seconds)
        encode_varint_field(&mut buf, 4, timestamp);
        // Field 6: client kind = "Web"
        encode_string(&mut buf, 6, "Web");
        buf
    }

    /// Build ReqHeartbeat for Route.heartbeat.
    ///
    /// `5000/5000` is not a placeholder — it is what the client sends as the
    /// *first* heartbeat of every connection, exactly, in all eleven connections
    /// across five captures. Every heartbeat after that carries a measured round
    /// trip instead: 110 of them span 1 to 1399 ms with a median of 253, and
    /// `network_quality` is a second measurement close to but rarely equal to
    /// `delay`.
    ///
    /// So the constant was right and the loop was missing. See
    /// [`super::MajsoulRpc::start_heartbeat`].
    pub fn build_heartbeat(delay_ms: u64, quality_ms: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, delay_ms); // delay
        encode_varint_field(&mut buf, 2, 0); // no_operation_counter
        encode_varint_field(&mut buf, 3, 11); // platform (Web)
        encode_varint_field(&mut buf, 4, quality_ms); // network_quality
        buf
    }

    /// The first heartbeat of a connection, byte for byte as captured.
    pub fn build_first_heartbeat() -> Vec<u8> {
        build_heartbeat(5_000, 5_000)
    }
}

/// Everything needed to send one request and wait for its answer.
///
/// Split out from [`MajsoulRpc`] so the heartbeat task can hold it: a heartbeat
/// has to share the socket, the pending map *and* the request counter with
/// ordinary calls, or the two would hand out the same message id.
#[derive(Clone)]
struct Channel {
    write: Arc<Mutex<futures_util::stream::SplitSink<WsStream, Message>>>,
    pending: Arc<Mutex<HashMap<u16, oneshot::Sender<Vec<u8>>>>>,
    req_idx: Arc<AtomicU16>,
    /// The last round trip a heartbeat measured, in milliseconds, which is what
    /// the next one reports. Starts at the 5000 the client opens every
    /// connection with.
    last_rtt: Arc<AtomicU64>,
}

pub struct MajsoulRpc {
    channel: Channel,
    _read_task: tokio::task::JoinHandle<()>,
    /// Aborted when the connection is dropped, which is what stops the
    /// heartbeat outliving the socket it beats on.
    heartbeat: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for MajsoulRpc {
    fn drop(&mut self) {
        if let Ok(mut handle) = self.heartbeat.try_lock()
            && let Some(handle) = handle.take()
        {
            handle.abort();
        }
    }
}

impl MajsoulRpc {
    /// Connect directly or through a fixed HTTP CONNECT proxy. When a proxy
    /// is supplied there is deliberately no DIRECT fallback.
    pub async fn connect_with_proxy(
        endpoint: &str,
        proxy: Option<&str>,
        chrome: u32,
    ) -> Result<Self> {
        let mut request = endpoint.into_client_request()?;
        {
            // What Chrome sends on a WebSocket upgrade, measured by pointing a
            // real headless Chrome at a bare socket and reading the request off
            // the wire. Note what is *not* here: no `sec-ch-ua`, no
            // `sec-fetch-*`, no `accept`. A browser attaches all of those to an
            // HTTP request and none of them to a WebSocket handshake, so adding
            // them would be as wrong as leaving these out.
            //
            // tungstenite writes Host, Connection, Upgrade, Sec-WebSocket-Key
            // and Sec-WebSocket-Version itself. Their order relative to these
            // cannot be set from here and does not match the browser's; that is
            // the one part of the handshake this cannot fix.
            let headers = request.headers_mut();
            headers.insert("Pragma", "no-cache".parse().unwrap());
            headers.insert("Cache-Control", "no-cache".parse().unwrap());
            headers.insert(
                "User-Agent",
                requests::user_agent(chrome).parse().context("user agent")?,
            );
            headers.insert("Origin", MS_HOST.parse().unwrap());
            headers.insert(
                "Accept-Encoding",
                "gzip, deflate, br, zstd".parse().unwrap(),
            );
            // Offered because the browser offers it. The gateway declines it —
            // its `101` carries no `Sec-WebSocket-Extensions` even when this is
            // sent — which matters, because tungstenite is built here without
            // compression support and would read deflated frames as garbage.
            // The handshake response is checked below rather than trusted.
            headers.insert(
                "Sec-WebSocket-Extensions",
                "permessage-deflate; client_max_window_bits"
                    .parse()
                    .unwrap(),
            );
        }

        debug!("Connecting to {}", endpoint);
        let (ws_stream, handshake) = if let Some(proxy) = proxy {
            let proxy_url = reqwest::Url::parse(proxy).context("Invalid proxy URL")?;
            if proxy_url.scheme() != "http" {
                anyhow::bail!("Only http:// CONNECT proxies are currently supported");
            }
            let proxy_host = proxy_url.host_str().context("Proxy URL has no host")?;
            let proxy_port = proxy_url
                .port_or_known_default()
                .context("Proxy URL has no port")?;
            let endpoint_url = reqwest::Url::parse(endpoint).context("Invalid WebSocket URL")?;
            let target_host = endpoint_url
                .host_str()
                .context("WebSocket URL has no host")?;
            let target_port = endpoint_url.port_or_known_default().unwrap_or(443);

            let mut tcp = TcpStream::connect((proxy_host, proxy_port))
                .await
                .context("Failed to connect to HTTP proxy")?;
            let mut connect = format!(
                "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\n"
            );
            if !proxy_url.username().is_empty() {
                let credentials = format!(
                    "{}:{}",
                    proxy_url.username(),
                    proxy_url.password().unwrap_or("")
                );
                let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
                connect.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
            }
            connect.push_str("Proxy-Connection: Keep-Alive\r\n\r\n");
            tcp.write_all(connect.as_bytes()).await?;

            let mut response = Vec::with_capacity(1024);
            let mut byte = [0u8; 1];
            while response.len() < 16 * 1024 && !response.ends_with(b"\r\n\r\n") {
                tcp.read_exact(&mut byte).await?;
                response.push(byte[0]);
            }
            let header = String::from_utf8_lossy(&response);
            let status = header.lines().next().unwrap_or_default();
            if !status.contains(" 200 ") {
                anyhow::bail!("HTTP proxy CONNECT failed: {}", status);
            }
            client_async_tls_with_config(request, tcp, None, None)
                .await
                .context("WebSocket TLS handshake through proxy failed")?
        } else {
            connect_async(request)
                .await
                .context("WebSocket connect failed")?
        };

        // Offering permessage-deflate is only safe while the gateway keeps
        // declining it. If it ever accepts, every frame after this arrives
        // deflated and tungstenite — built here without compression — reads
        // them as malformed. Failing here turns that into one clear error
        // instead of a collector that silently stops understanding the server.
        if let Some(accepted) = handshake.headers().get("sec-websocket-extensions") {
            anyhow::bail!(
                "gateway accepted a WebSocket extension this client cannot read \
                 ({}); stop offering permessage-deflate in connect_with_proxy",
                accepted.to_str().unwrap_or("unreadable")
            );
        }

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));
        let pending: Arc<Mutex<HashMap<u16, oneshot::Sender<Vec<u8>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_clone = Arc::clone(&pending);
        let read_task = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Binary(data)) if data.len() >= 3 && data[0] == 3 => {
                        // RESPONSE
                        let idx = u16::from_le_bytes([data[1], data[2]]);
                        if let Ok((_, response_data)) = wrapper::decode(&data[3..]) {
                            let mut pending = pending_clone.lock().await;
                            if let Some(tx) = pending.remove(&idx) {
                                let _ = tx.send(response_data);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        debug!("WebSocket closed");
                        break;
                    }
                    Err(e) => {
                        warn!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            // Wake all RPC callers immediately when the peer disappears;
            // otherwise each login worker waits for the full RPC timeout.
            pending_clone.lock().await.clear();
        });

        debug!("Connected to Majsoul gateway");
        Ok(Self {
            channel: Channel {
                write,
                pending,
                req_idx: Arc::new(AtomicU16::new(1)),
                last_rtt: Arc::new(AtomicU64::new(5_000)),
            },
            _read_task: read_task,
            heartbeat: Mutex::new(None),
        })
    }

    pub async fn call(&self, method: &str, request_data: &[u8]) -> Result<Vec<u8>> {
        self.channel.call(method, request_data).await
    }

    /// Starts beating for the life of this connection.
    ///
    /// The cadence is the client's own, measured off five captures: six beats
    /// half a second apart as the connection settles, then one every fifteen
    /// seconds until the socket goes. Every connection in every capture does
    /// this, including the ones the client opens to gateways it never logs in
    /// through.
    ///
    /// What this replaced sent exactly one heartbeat, before the login, and then
    /// nothing — so a collector session that stayed up for six hours sent one
    /// beat in six hours. There is no rate limit to hide behind here and no cost
    /// to paying it: the difference between a client and this was a query on
    /// "sessions with no heartbeat in the last minute".
    ///
    /// Failures end the task rather than being reported. A heartbeat that cannot
    /// be sent means the socket is gone, and the caller will find that out from
    /// its own next request — which is the error worth surfacing.
    pub async fn start_heartbeat(&self) {
        let channel = self.channel.clone();
        let task = tokio::spawn(async move {
            // The settling burst. The first beat of the connection is sent by
            // `route_connect`, so this picks up at the second.
            for _ in 0..5 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if channel.beat().await.is_err() {
                    return;
                }
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if channel.beat().await.is_err() {
                    return;
                }
            }
        });
        if let Some(previous) = self.heartbeat.lock().await.replace(task) {
            previous.abort();
        }
    }
}

impl Channel {
    async fn call(&self, method: &str, request_data: &[u8]) -> Result<Vec<u8>> {
        let idx = self.req_idx.fetch_add(1, Ordering::SeqCst) % 60007;

        let wrapped = wrapper::encode(method, request_data);
        let mut packet = vec![0x02];
        packet.extend_from_slice(&idx.to_le_bytes());
        packet.extend_from_slice(&wrapped);

        let (tx, rx) = oneshot::channel();
        {
            self.pending.lock().await.insert(idx, tx);
        }
        {
            self.write
                .lock()
                .await
                .send(Message::Binary(packet))
                .await?;
        }

        debug!("Sent RPC: {} (idx={})", method, idx);

        let response = tokio::time::timeout(std::time::Duration::from_secs(30), rx)
            .await
            .context("RPC timeout")?
            .context("RPC channel closed")?;
        Ok(response)
    }

    /// One heartbeat carrying the round trip the previous one measured.
    ///
    /// Measured rather than invented, because the captured values are a real
    /// distribution — 1 to 1399 ms, median 253 — and a constant, or a random
    /// number from a range, is a distribution of its own.
    async fn beat(&self) -> Result<()> {
        let started = std::time::Instant::now();
        // Seeded from the last measurement the same way the client does: the
        // first of a connection is 5000/5000 and every later one reports what
        // was actually observed.
        // ponytail: delay and network_quality get the same number. The captures
        // have them equal in 3 of 8 samples and within a quarter otherwise, so
        // this sits inside the real distribution; split them if that ever stops
        // being true.
        let previous = self.last_rtt.load(Ordering::Relaxed).clamp(1, 5_000);
        self.call(
            ".lq.Route.heartbeat",
            &requests::build_heartbeat(previous, previous),
        )
        .await?;
        let elapsed = started.elapsed().as_millis().clamp(1, 5_000) as u64;
        self.last_rtt.store(elapsed, Ordering::Relaxed);
        Ok(())
    }
}

impl MajsoulRpc {
    pub async fn fetch_game_record(&self, uuid: &str, version: &str) -> Result<Vec<u8>> {
        let request = build_fetch_game_record_request(uuid, version);
        let response = self.call(FETCH_GAME_RECORD_METHOD, &request).await?;
        // Check for error: direct (08 XX) or nested (0a LL 08 XX)
        if let Some(code) = Self::extract_error_code(&response) {
            if code != 0 {
                return Err(anyhow::Error::new(ServerError { code })
                    .context(format!("fetchGameRecord error {}: {}", code, uuid)));
            }
        }
        debug!("Fetched game record: {} ({} bytes)", uuid, response.len());
        Ok(response)
    }

    /// Fetch the in-progress game list for one filter (`mode_id + 200`).
    /// Returns the raw ResGameLiveList bytes for the caller to decode.
    pub async fn fetch_game_live_list(&self, filter_id: u32) -> Result<Vec<u8>> {
        let request = build_fetch_game_live_list_request(filter_id);
        let response = self.call(FETCH_GAME_LIVE_LIST_METHOD, &request).await?;
        if let Some(code) = Self::extract_error_code(&response) {
            if code != 0 {
                return Err(anyhow::Error::new(ServerError { code }).context(format!(
                    "fetchGameLiveList error {} for filter {}",
                    code, filter_id
                )));
            }
        }
        Ok(response)
    }

    /// Perform route connection handshake (required before login)
    pub async fn route_connect(&self, route_id: &str) -> Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        debug!(
            "Sending route connection (route_id: {}, timestamp: {})",
            route_id, timestamp
        );

        let request = requests::build_request_connection(route_id, timestamp);
        let response = self.call(".lq.Route.requestConnection", &request).await?;

        // Check for error
        if let Some(code) = Self::extract_error_code(&response) {
            if code != 0 {
                return Err(anyhow::Error::new(ServerError { code })
                    .context(format!("Route connection failed (error {})", code)));
            }
        }

        debug!("Route connection established");
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(handle) = self.heartbeat.lock().await.take() {
            handle.abort();
        }
        self.channel.write.lock().await.close().await?;
        Ok(())
    }

    /// Login with the exact client_version_string used by the official web
    /// client (for example `WebGL_2022-0.16.251`).
    pub async fn login_native_exact(
        &self,
        username: &str,
        password: &str,
        code_version: &str,
        package_version: &str,
        tag: &str,
        route_id: &str,
    ) -> Result<u64> {
        use crate::majsoul::auth::hash_password;

        // Step 1: Route connection handshake (CRITICAL - required before login)
        self.route_connect(route_id).await?;

        // Step 2: Match the current official client: heartbeat belongs to
        // Route, not the legacy misspelled Lobby.heatbeat method.
        debug!("Sending route heartbeat");
        let heartbeat = requests::build_first_heartbeat();
        let hb_response = self.call(".lq.Route.heartbeat", &heartbeat).await?;
        debug!("Heartbeat response: {} bytes", hb_response.len());

        // And then keep beating, which is what the client does and what this
        // never did. Started here rather than after the login so the settling
        // burst overlaps the login the way the captures show it doing.
        self.start_heartbeat().await;

        let password_hash = hash_password(password);
        // The machine's id, and it does not change.
        //
        // This was a fresh `Uuid::new_v4()` on every login, which for a pool
        // account reconnecting every few minutes reads as the same person
        // logging in from a new device all day. It is also the `device_id` the
        // client reports in its telemetry, so it has to be something an account
        // *has* rather than something a connection makes up.
        let random_key = super::device_id(username);
        debug!("Authenticating with native login (account={})", username);

        // Build ReqLogin protobuf
        let request = requests::build_login_request(
            username,
            &password_hash,
            &random_key,
            code_version,
            package_version,
            tag,
        );

        let response = self.call(".lq.Lobby.login", &request).await?;

        debug!(
            "login response ({} bytes): {:02x?}",
            response.len(),
            &response[..std::cmp::min(100, response.len())]
        );

        // Check for error
        if let Some(code) = Self::extract_error_code(&response) {
            if code != 0 {
                return Err(anyhow::Error::new(ServerError { code })
                    .context(format!("CN native login failed with error code: {}", code)));
            }
        }

        // Extract access_token (field 2) for verification
        if let Some(_token) = Self::extract_string_field(&response, 2) {
            debug!("Login successful (account={})", username);
        } else {
            debug!("Login successful (account={}, no token)", username);
        }
        // `ResLogin.account_id`, field 2 as a varint. The telemetry cannot be
        // sent without it, and it is the one field of the answer that is not
        // this deployment's own to invent. Field 2 is also where the access
        // token lives when it is a string, so the wire type has to be checked
        // rather than assumed.
        let account_id = crate::majsoul::proto::FieldIterator::new(&response)
            .flatten()
            .find(|field| field.number == 2 && field.wire_type == 0)
            .and_then(|field| crate::majsoul::proto::extract_varint(field.data).ok())
            .unwrap_or(0);

        self.settle_into_the_lobby().await;
        Ok(account_id)
    }

    /// What the client does in the fifteen seconds after a login.
    ///
    /// Read off `captures/register_20260805_174145.json`, which is a real
    /// browser: `fetchLastPrivacy`, two `loginBeat`s, then the lobby's own
    /// contents — announcement, account info, the questionnaire, the challenge
    /// pair, the seer report, the revive coin, the daily task — then, after a
    /// pause, `fetchConnectionInfo` and `fetchRollingNotice`, and `loginSuccess`
    /// only at the end.
    ///
    /// What this replaced was `login` → `loginSuccess` → `loginBeat`, three
    /// frames inside fifty milliseconds, and then nothing but `fetchGameRecord`
    /// for as long as the session lived. No browser produces that: the lobby is
    /// a screen, and a client that logs in never draws it.
    ///
    /// Nothing here is checked and nothing here can fail the login. These are
    /// reads whose answers are thrown away — the session is already up by the
    /// time they run, and losing one to a refusal is not worth ending a
    /// collector over. The delays are the captured ones rather than random,
    /// because the shape is what is being copied.
    async fn settle_into_the_lobby(&self) {
        use std::time::Duration;
        const CONTRACT: &str = "DF2vkXCnfeXp4WoGrBGNcJBufZiMN3uP";

        // A constant embedded in the client, not a secret and not per session:
        // fifteen frames across five captures, five accounts and three weeks all
        // carry this exact string. What was here before differed from it at two
        // positions (index 16 `r`→`S`, index 30 `u`→`U`), which means every
        // login this deployment ever made was identifiable by one string
        // comparison — no fingerprinting, no correlation, just a value no real
        // client sends.
        let beat = requests::build_login_beat_request(CONTRACT);

        // (method, how long to wait before sending it) — the gaps the capture
        // shows, to a tenth of a second.
        let steps: [(&str, &[u8], u64); 14] = [
            (".lq.Lobby.fetchLastPrivacy", &[], 860),
            (".lq.Lobby.loginBeat", &beat, 340),
            (".lq.Lobby.loginBeat", &beat, 1_220),
            (".lq.Lobby.fetchAnnouncement", &[], 5_440),
            (".lq.Lobby.fetchInfo", &[], 350),
            (".lq.Lobby.fetchQuestionnaireList", &[], 280),
            (".lq.Lobby.fetchChallengeInfo", &[], 10),
            (".lq.Lobby.fetchChallengeSeason", &[], 10),
            (".lq.Lobby.fetchSeerReportList", &[], 10),
            (".lq.Lobby.fetchReviveCoinInfo", &[], 40),
            (".lq.Lobby.fetchDailyTask", &[], 10),
            (".lq.Lobby.fetchConnectionInfo", &[], 12_280),
            (".lq.Lobby.fetchRollingNotice", &[], 5_000),
            // Last, not first. In the capture it lands with three other frames
            // in the same millisecond, twenty seconds after the lobby is drawn.
            (".lq.Lobby.loginSuccess", &[], 890),
        ];

        for (method, body, wait_ms) in steps {
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            if let Err(error) = self.call(method, body).await {
                debug!("lobby settle step {method} did not answer: {error:#}");
                return;
            }
        }
    }

    /// Extract error code from protobuf response
    /// Handles both nested (0a <len> 08 <code>) and direct (08 <code>)
    /// formats. The code is a varint: values above 127 span several bytes,
    /// so reading a single raw byte would truncate them (1203 -> "179").
    fn extract_error_code(data: &[u8]) -> Option<u64> {
        let payload = if data.first() == Some(&0x0a) {
            // Nested Error message in field 1
            let (len, len_bytes) = wrapper::decode_varint(&data[1..]).ok()?;
            let start = 1 + len_bytes;
            let end = start.checked_add(len as usize)?;
            data.get(start..end)?
        } else {
            data
        };
        if payload.first() == Some(&0x08) {
            let (code, _) = wrapper::decode_varint(&payload[1..]).ok()?;
            Some(code)
        } else {
            None
        }
    }

    /// Extract string field from protobuf response by field number
    fn extract_string_field(data: &[u8], target_field: u32) -> Option<String> {
        let mut pos = 0;
        while pos < data.len() {
            let tag = data[pos];
            pos += 1;
            let field_num = (tag >> 3) as u32;
            let wire_type = tag & 0x07;

            if wire_type == 2 {
                // Length-delimited
                let mut len: usize = 0;
                let mut shift = 0;
                while pos < data.len() {
                    let b = data[pos];
                    pos += 1;
                    len |= ((b & 0x7f) as usize) << shift;
                    if b & 0x80 == 0 {
                        break;
                    }
                    shift += 7;
                }
                if field_num == target_field && pos + len <= data.len() {
                    return Some(String::from_utf8_lossy(&data[pos..pos + len]).to_string());
                }
                pos += len;
            } else if wire_type == 0 {
                // Varint - skip
                while pos < data.len() && data[pos] & 0x80 != 0 {
                    pos += 1;
                }
                pos += 1;
            } else {
                // Unknown wire type, stop parsing
                break;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::MajsoulRpc;
    use super::requests;

    /// The first heartbeat of a connection is the captured constant, and every
    /// one after it reports a measurement.
    ///
    /// Both halves are pinned because getting either wrong is invisible. The
    /// constant looked like a placeholder and is not — eleven connections across
    /// five captures open with exactly `5000/5000`. And a constant *after* the
    /// first is the giveaway, because the client's later beats span 1 to 1399 ms
    /// with a median of 253.
    #[test]
    fn the_first_heartbeat_is_the_captured_constant_and_later_ones_are_not() {
        // f1=5000 f2=0 f3=11 f4=5000, the payload seen 11/11 times.
        assert_eq!(
            requests::build_first_heartbeat(),
            vec![0x08, 0x88, 0x27, 0x10, 0x00, 0x18, 0x0b, 0x20, 0x88, 0x27]
        );
        assert_eq!(
            requests::build_first_heartbeat(),
            requests::build_heartbeat(5_000, 5_000)
        );
        // A measured beat differs from it, which is the whole point.
        assert_ne!(
            requests::build_heartbeat(253, 253),
            requests::build_first_heartbeat()
        );
    }

    /// An empty request body is still written as a field.
    ///
    /// `loginSuccess` is the one every session sends and the one this used to
    /// get wrong: protobuf reads `12 00` and a missing field 2 as the same
    /// message, so nothing here ever failed — the difference only exists on the
    /// wire, where it is two bytes no browser is missing.
    #[test]
    fn an_empty_body_is_still_written_as_field_two() {
        let frame = super::wrapper::encode(".lq.Lobby.loginSuccess", &[]);
        assert_eq!(
            &frame[frame.len() - 2..],
            &[0x12, 0x00],
            "an empty body must end in 12 00: {frame:02x?}"
        );
        // And a real body is unchanged: field 2, its length, then the bytes.
        let carried = super::wrapper::encode(".lq.Lobby.login", &[0xaa, 0xbb]);
        assert_eq!(&carried[carried.len() - 4..], &[0x12, 0x02, 0xaa, 0xbb]);
        // Round-trips either way, which is what makes the omission invisible.
        let (name, data) = super::wrapper::decode(&frame).unwrap();
        assert_eq!(name, ".lq.Lobby.loginSuccess");
        assert!(data.is_empty());
    }

    /// The handshake says `Web`, capital W.
    ///
    /// Pinned to the captured bytes because the wrong case is invisible: the
    /// gateway accepts `web` and the session goes on to work, so nothing fails
    /// and nothing is logged — the only consequence is that every session this
    /// deployment opens carries a spelling no real client uses.
    #[test]
    fn the_route_handshake_spells_web_the_way_the_client_does() {
        let frame = requests::build_request_connection("route-2", 1_785_922_697);
        // Field 6, length 3, "Web" — `32 03 57 65 62` in three 2026-08 captures.
        assert!(
            frame
                .windows(5)
                .any(|w| w == [0x32, 0x03, 0x57, 0x65, 0x62]),
            "requestConnection field 6 must be \"Web\": {frame:02x?}"
        );
    }

    /// One machine per account, and the same one every time.
    ///
    /// Both halves are the point. Identical personas across accounts collect the
    /// whole pool in one query; a persona that moves between logins of one
    /// account is a machine that changed its screen size overnight.
    #[test]
    fn every_account_reports_its_own_stable_machine() {
        let a = requests::persona("alice@example.com");
        let b = requests::persona("bob@example.com");
        assert_ne!(a, b, "two accounts must not report the same machine");
        assert_eq!(
            a,
            requests::persona("alice@example.com"),
            "an account's machine must not change between logins"
        );
        // Case is not identity: Mahjong Soul treats these as one account, and so
        // does the pool's duplicate check, so they must get one machine.
        assert_eq!(a, requests::persona("Alice@Example.com"));

        let requests::Persona {
            chrome,
            viewport_width: width,
            viewport_height: height,
            gpu,
        } = a;
        assert!((149..=151).contains(&chrome));
        assert!(gpu.starts_with("ANGLE ("), "odd gpu string {gpu}");
        // A viewport, not a screen: shorter than any screen in the table by the
        // browser's own furniture.
        assert!(
            (500..=1_400).contains(&height),
            "odd viewport height {height}"
        );
        assert!(
            (1_280..=2_560).contains(&width),
            "odd viewport width {width}"
        );
        assert!(requests::user_agent(chrome).contains(&format!("Chrome/{chrome}.0.0.0")));
        assert!(requests::user_agent(chrome).contains("Macintosh"));
    }

    /// An account's device id is stable, distinct, and shaped like a uuid.
    ///
    /// Stable because it is the machine's identity in two places at once — the
    /// login's `random_key` and the telemetry's `device_id` — and a value that
    /// changed per connection described an account acquiring a new computer
    /// every few minutes. Shaped like a v4 uuid because anything parsing it
    /// should see nothing remarkable.
    #[test]
    fn an_accounts_device_id_is_stable_and_looks_like_any_other_uuid() {
        let a = super::super::device_id("alice@example.com");
        assert_eq!(a, super::super::device_id("alice@example.com"));
        assert_eq!(a, super::super::device_id("Alice@Example.COM"));
        assert_ne!(a, super::super::device_id("bob@example.com"));

        let parsed = uuid::Uuid::parse_str(&a).expect("a uuid");
        assert_eq!(
            parsed.get_version_num(),
            4,
            "must read as random, not hashed"
        );
        assert_eq!(a.len(), 36);
    }

    /// The device is the desktop web client's, field for field.
    ///
    /// What this replaced claimed to be Android and disagreed with all five
    /// captures in three checkable ways, the worst of which — a field 4 the real
    /// client never sends — is present or absent, not a matter of degree.
    #[test]
    fn the_login_device_matches_the_captured_web_client() {
        let frame = requests::build_login_request(
            "someone@example.com",
            "0".repeat(64).as_str(),
            "random",
            "0.16.257",
            "4.0.45",
            "cn",
        );
        let contains = |needle: &[u8]| frame.windows(needle.len()).any(|w| w == needle);
        // f1="pc" f2="pc" f3="mac" f7="web" (lower case here, unlike the
        // handshake's "Web"), screen_type f13=1.
        assert!(contains(b"\x0a\x02pc"), "device f1 must be pc");
        assert!(contains(b"\x12\x02pc"), "device f2 must be pc");
        assert!(contains(b"\x1a\x03mac"), "device f3 must be mac");
        assert!(contains(b"\x3a\x03web"), "device f7 must be lower-case web");
        assert!(
            contains(&[0x68, 0x01]),
            "device f13 (screen_type) must be 1"
        );
        assert!(
            !contains(b"android"),
            "the captured client is not an Android one"
        );
        // The login frame's own user agent has to be the one `user_agent` gives
        // for this account, because that is what the HTTP side of the same
        // session sends. This is the assertion that fails if the two ever drift
        // apart again — which is exactly what a fleet-wide HTTP constant did.
        let chrome = requests::persona("someone@example.com").chrome;
        assert!(
            contains(requests::user_agent(chrome).as_bytes()),
            "device f12 must be the account's own user agent"
        );
    }

    #[test]
    fn decodes_multibyte_error_codes() {
        // Nested Error{code=1203} in field 1: 0a 03 08 b3 09.
        assert_eq!(
            MajsoulRpc::extract_error_code(&[0x0a, 0x03, 0x08, 0xb3, 0x09]),
            Some(1203)
        );
        // Direct single-byte code.
        assert_eq!(MajsoulRpc::extract_error_code(&[0x08, 0x02]), Some(2));
        // Direct two-byte varint code.
        assert_eq!(
            MajsoulRpc::extract_error_code(&[0x08, 0xb3, 0x01]),
            Some(179)
        );
        // Field 1 is length-delimited data, not an Error message.
        assert_eq!(
            MajsoulRpc::extract_error_code(&[0x0a, 0x02, 0x12, 0x00]),
            None
        );
        assert_eq!(MajsoulRpc::extract_error_code(&[]), None);
        // Truncated varint must not panic or invent a code.
        assert_eq!(MajsoulRpc::extract_error_code(&[0x08, 0xb3]), None);
        assert_eq!(
            MajsoulRpc::extract_error_code(&[0x0a, 0x05, 0x08, 0xb3]),
            None
        );
    }

    #[test]
    fn rejects_overlong_varints_without_panicking() {
        // 11-byte non-canonical varint would shift past 64 bits; the parser
        // must return None instead of panicking (debug) or wrapping (release).
        let mut hostile = vec![0x08];
        hostile.extend(std::iter::repeat_n(0x80, 10));
        hostile.push(0x01);
        assert_eq!(MajsoulRpc::extract_error_code(&hostile), None);
        // Same bytes as a nested field-1 length.
        let mut nested = vec![0x0a];
        nested.extend(std::iter::repeat_n(0x80, 10));
        nested.push(0x01);
        assert_eq!(MajsoulRpc::extract_error_code(&nested), None);
        // A 10th byte carrying bits beyond u64 must also be rejected.
        let mut too_wide = vec![0x08];
        too_wide.extend(std::iter::repeat_n(0xff, 9));
        too_wide.push(0x7f);
        assert_eq!(MajsoulRpc::extract_error_code(&too_wide), None);
    }
}
