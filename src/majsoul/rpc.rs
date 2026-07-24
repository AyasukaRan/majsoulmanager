use anyhow::{Context, Result};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, warn};
use uuid::Uuid;

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

    pub fn encode(name: &str, data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        // Field 1: name (string)
        buf.push(0x0a);
        encode_varint(&mut buf, name.len() as u64);
        buf.extend_from_slice(name.as_bytes());
        // Field 2: data (bytes)
        if !data.is_empty() {
            buf.push(0x12);
            encode_varint(&mut buf, data.len() as u64);
            buf.extend_from_slice(data);
        }
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

mod requests {
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

    /// Build ReqLogin for CN native login (username/password), matching the
    /// fields a real web client sends so risk control does not reject it.
    /// Field numbers from protobuf: account=1, password=2, device=4,
    /// random_key=5, client_version=6, gen_access_token=7,
    /// currency_platforms=8, client_version_string=11.
    pub fn build_login_request(
        account: &str,
        password_hash: &str,
        random_key: &str,
        client_version_string: &str,
        resource: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1: account (string)
        encode_string(&mut buf, 1, account);
        // Field 2: password (string, hashed)
        encode_string(&mut buf, 2, password_hash);
        // Field 4: device — full ClientDeviceInfo like a real browser
        encode_device(&mut buf);
        // Field 5: random_key (string)
        encode_string(&mut buf, 5, random_key);
        // Field 6: client_version { resource } — full version with `.w`
        encode_client_version(&mut buf, resource);
        // Field 7: gen_access_token (bool = true)
        encode_bool(&mut buf, 7, true);
        // Field 8: currency_platforms (repeated int32 = [2])
        encode_varint_field(&mut buf, 8, 2);
        // Field 11: client_version_string (web-<version>)
        encode_string(&mut buf, 11, client_version_string);

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
        let tag = (field << 3) | 0;
        encode_varint(buf, tag as u64);
        buf.push(if value { 1 } else { 0 });
    }

    fn encode_varint_field(buf: &mut Vec<u8>, field: u32, value: u64) {
        let tag = (field << 3) | 0;
        encode_varint(buf, tag as u64);
        encode_varint(buf, value);
    }

    /// Encode ClientDeviceInfo (field 4) with the fields a real web client
    /// sends, so risk control does not flag the login as a bare script.
    fn encode_device(buf: &mut Vec<u8>) {
        let mut inner = Vec::new();
        encode_string(&mut inner, 1, "pc"); // platform
        encode_string(&mut inner, 2, "pc"); // hardware
        encode_string(&mut inner, 3, "windows"); // os
        encode_string(&mut inner, 4, "win10"); // os_version
        encode_bool(&mut inner, 5, true); // is_browser
        encode_string(&mut inner, 6, "Chrome"); // software
        encode_string(&mut inner, 7, "web"); // sale_platform

        let tag = (4 << 3) | 2;
        encode_varint(buf, tag as u64);
        encode_varint(buf, inner.len() as u64);
        buf.extend(inner);
    }

    /// Encode ClientVersionInfo (field 6) { resource } — the full version
    /// string keeping the trailing `.w`, exactly as the real client sends.
    fn encode_client_version(buf: &mut Vec<u8>, resource: &str) {
        let mut inner = Vec::new();
        encode_string(&mut inner, 1, resource); // resource

        let tag = (6 << 3) | 2;
        encode_varint(buf, tag as u64);
        encode_varint(buf, inner.len() as u64);
        buf.extend(inner);
    }

    /// Build ReqRequestConnection for route handshake (required before login)
    /// Field numbers from protobuf: type=2, route_id=3, timestamp=4
    pub fn build_request_connection(route_id: &str, timestamp: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        // Field 2: type = 3 (varint)
        encode_varint_field(&mut buf, 2, 3);
        // Field 3: route_id (string)
        encode_string(&mut buf, 3, route_id);
        // Field 4: timestamp (varint, milliseconds)
        encode_varint_field(&mut buf, 4, timestamp);
        buf
    }

    /// Build ReqHeartbeat for Route.heartbeat
    pub fn build_heartbeat() -> Vec<u8> {
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, 0); // delay
        encode_varint_field(&mut buf, 2, 0); // no_operation_counter
        encode_varint_field(&mut buf, 3, 11); // platform (Web)
        encode_varint_field(&mut buf, 4, 0); // network_quality
        buf
    }
}

pub struct MajsoulRpc {
    write: Arc<Mutex<futures_util::stream::SplitSink<WsStream, Message>>>,
    pending: Arc<Mutex<HashMap<u16, oneshot::Sender<Vec<u8>>>>>,
    req_idx: AtomicU16,
    _read_task: tokio::task::JoinHandle<()>,
}

impl MajsoulRpc {
    /// Connect directly or through a fixed HTTP CONNECT proxy. When a proxy
    /// is supplied there is deliberately no DIRECT fallback.
    pub async fn connect_with_proxy(endpoint: &str, proxy: Option<&str>) -> Result<Self> {
        let mut request = endpoint.into_client_request()?;
        request
            .headers_mut()
            .insert("Origin", MS_HOST.parse().unwrap());

        debug!("Connecting to {}", endpoint);
        let (ws_stream, _) = if let Some(proxy) = proxy {
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

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));
        let pending: Arc<Mutex<HashMap<u16, oneshot::Sender<Vec<u8>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_clone = Arc::clone(&pending);
        let read_task = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Binary(data)) if data.len() >= 3 => {
                        if data[0] == 3 {
                            // RESPONSE
                            let idx = u16::from_le_bytes([data[1], data[2]]);
                            if let Ok((_, response_data)) = wrapper::decode(&data[3..]) {
                                let mut pending = pending_clone.lock().await;
                                if let Some(tx) = pending.remove(&idx) {
                                    let _ = tx.send(response_data);
                                }
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
            write,
            pending,
            req_idx: AtomicU16::new(1),
            _read_task: read_task,
        })
    }

    pub async fn call(&self, method: &str, request_data: &[u8]) -> Result<Vec<u8>> {
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
                .send(Message::Binary(packet.into()))
                .await?;
        }

        debug!("Sent RPC: {} (idx={})", method, idx);

        let response = tokio::time::timeout(std::time::Duration::from_secs(30), rx)
            .await
            .context("RPC timeout")?
            .context("RPC channel closed")?;
        Ok(response)
    }

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
            .as_millis() as u64;

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
        self.write.lock().await.close().await?;
        Ok(())
    }

    /// Login with the exact client_version_string used by the official web
    /// client (for example `WebGL_2022-0.16.251`).
    pub async fn login_native_exact(
        &self,
        username: &str,
        password: &str,
        client_version: &str,
        resource: &str,
        route_id: &str,
    ) -> Result<()> {
        use crate::majsoul::auth::hash_password;

        // Step 1: Route connection handshake (CRITICAL - required before login)
        self.route_connect(route_id).await?;

        // Step 2: Match the current official client: heartbeat belongs to
        // Route, not the legacy misspelled Lobby.heatbeat method.
        debug!("Sending route heartbeat");
        let heartbeat = requests::build_heartbeat();
        let hb_response = self.call(".lq.Route.heartbeat", &heartbeat).await?;
        debug!("Heartbeat response: {} bytes", hb_response.len());

        let password_hash = hash_password(password);
        let random_key = Uuid::new_v4().to_string();
        debug!("Authenticating with native login (account={})", username);

        // Build ReqLogin protobuf
        let request = requests::build_login_request(
            username,
            &password_hash,
            &random_key,
            client_version,
            resource,
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

        // Send loginSuccess
        self.call(".lq.Lobby.loginSuccess", &[]).await?;

        // Send loginBeat with contract
        let contract = "DF2vkXCnfeXp4WoGSBGNcJBufZiMN3UP";
        let beat_req = requests::build_login_beat_request(contract);
        self.call(".lq.Lobby.loginBeat", &beat_req).await?;

        Ok(())
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
