use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Get base URL for server
pub fn server_base_url(server: &str) -> &'static str {
    match server {
        "cn" => "https://game.maj-soul.com",
        "en" | "jp" => "https://mahjongsoul.game.yo-star.com",
        _ => "https://mahjongsoul.game.yo-star.com",
    }
}

/// Get path prefix for server (CN uses /1/, EN doesn't)
fn server_path_prefix(server: &str) -> &'static str {
    match server {
        "cn" => "/1",
        _ => "",
    }
}

#[derive(Debug, Deserialize)]
pub struct VersionInfo {
    pub version: String,
}

/// resversion{version}.json manifest: maps each resource path to the version
/// prefix under which it is served. The reference web client resolves
/// config.json (and the protobuf definition) through this map instead of
/// guessing the path, so the URLs follow every Majsoul release automatically.
#[derive(Debug, Deserialize)]
struct ResVersion {
    res: HashMap<String, ResEntry>,
}

#[derive(Debug, Deserialize)]
struct ResEntry {
    prefix: String,
}

#[derive(Debug, Deserialize)]
struct Gateway {
    url: String,
}

#[derive(Debug, Deserialize)]
struct IpEntry {
    #[serde(default)]
    name: String,
    #[serde(default)]
    gateways: Vec<Gateway>,
}

#[derive(Debug, Deserialize)]
struct Config {
    ip: Vec<IpEntry>,
}

/// Route entry from /api/clientgate/routes response
#[derive(Debug, Deserialize)]
struct Route {
    domain: String,
    id: String,
}

/// Inner data from /api/clientgate/routes
#[derive(Debug, Deserialize)]
struct RoutesData {
    routes: Vec<Route>,
}

/// Routes response from /api/clientgate/routes
#[derive(Debug, Deserialize)]
struct RoutesResponse {
    data: RoutesData,
}

/// Fetch a resource as text, falling back to the last cached copy on failure.
///
/// Network-first with a content-addressed cache fallback (a deliberate
/// variation on the reference client's cache-first `getRes`, chosen so
/// discovery always returns fresh data when the CDN is reachable): successful
/// responses are written to the cache atomically, and if the network or a
/// non-2xx status fails, the previous cached body (if any) is returned so a
/// transient CDN hiccup does not stall discovery. `bust_cache` appends a
/// random query parameter and `If-Modified-Since: 0` to bypass CDN caching
/// for endpoints that must always be current (version.json, routes).
async fn fetch_text_cached(
    client: &reqwest::Client,
    url: &str,
    cache_dir: &Path,
    bust_cache: bool,
    timeout: Duration,
) -> Result<String> {
    let cache_key = format!("{:x}", Sha256::digest(url.as_bytes()));
    let cache_file = cache_dir.join(cache_key);

    let request_url = if bust_cache {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}randv={}", Uuid::new_v4().simple())
    } else {
        url.to_owned()
    };

    let fetched = async {
        let mut request = client.get(&request_url);
        if bust_cache {
            request = request.header("If-Modified-Since", "0");
        }
        let response = tokio::time::timeout(timeout, request.send())
            .await
            .context("request timed out")??
            .error_for_status()?;
        let text = tokio::time::timeout(timeout, response.text())
            .await
            .context("body read timed out")??;
        anyhow::Ok(text)
    }
    .await;

    match fetched {
        Ok(text) => {
            write_cache_atomic(&cache_file, &text);
            Ok(text)
        }
        Err(error) => match std::fs::read_to_string(&cache_file) {
            Ok(cached) => {
                warn!("Using cached copy of {url} after fetch failure: {error:#}");
                Ok(cached)
            }
            Err(_) => {
                Err(error.context(format!("failed to fetch {url} and no cached copy exists")))
            }
        },
    }
}

/// JSON variant of [`fetch_text_cached`].
async fn fetch_json_cached<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    cache_dir: &Path,
    bust_cache: bool,
) -> Result<T> {
    let raw = fetch_text_cached(client, url, cache_dir, bust_cache, REQUEST_TIMEOUT).await?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse JSON from {url}"))
}

fn write_cache_atomic(cache_file: &Path, contents: &str) {
    let Some(dir) = cache_file.parent() else {
        return;
    };
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    let temp = cache_file.with_extension(Uuid::new_v4().simple().to_string());
    if std::fs::write(&temp, contents).is_ok() {
        let _ = std::fs::rename(&temp, cache_file);
    }
}

/// Resolve the config.json URL through the resversion manifest, exactly like
/// the official web client. Returns an error if the manifest is unavailable
/// so the caller can fall back to the legacy path.
async fn resolve_config_url(
    client: &reqwest::Client,
    ms_host: &str,
    prefix: &str,
    version: &str,
    cache_dir: &Path,
) -> Result<String> {
    let resversion_url = format!("{ms_host}{prefix}/resversion{version}.json");
    let manifest: ResVersion = fetch_json_cached(client, &resversion_url, cache_dir, false).await?;
    let config_prefix = manifest
        .res
        .get("config.json")
        .map(|entry| entry.prefix.clone())
        .context("resversion manifest has no config.json entry")?;
    Ok(format!("{ms_host}{prefix}/{config_prefix}/config.json"))
}

/// Discover gateway endpoint, version, and route_id for Majsoul server
///
/// Returns (endpoint, version, route_id) tuple needed for connection.
/// `cache_dir` backs a content-addressed resource cache used as a fallback
/// when the live CDN is briefly unavailable.
/// The route hosts, as the client build carries them.
///
/// Not discovered, because the real client does not discover them. Five
/// captures of a browser playing the 4.0.45 CN client contain 1389 HTTP
/// requests and not one of them is `version.json`, `resversion*.json` or
/// `config.json` — the gateway list is compiled in, and the client goes
/// straight to `/api/clientgate/routes` on each of these.
///
/// Kept in the order the client asks them in, which is also the order to prefer.
const CN_ROUTE_HOSTS: [&str; 5] = [
    "https://route-2.maj-soul.com",
    "https://route-3.maj-soul.com:8443",
    "https://route-4.maj-soul.com",
    "https://route-5.maj-soul.com",
    "https://route-6.maj-soul.com",
];

/// Builds the `routes` URL exactly as the client does.
///
/// `version` is the *package* version (`4.0.45`) — the Unity build — not the
/// resource version (`0.16.257`). This sent the resource version, which is a
/// value no client ever puts in this parameter, and omitted `lang` entirely.
fn routes_url(host: &str, package_version: &str) -> String {
    format!("{host}/api/clientgate/routes?platform=Web&version={package_version}&lang=chs_t")
}

pub async fn discover_gateway(
    client: &reqwest::Client,
    server: &str,
    cache_dir: &Path,
    package_version: &str,
) -> Result<(String, String, String)> {
    let ms_host = server_base_url(server);
    info!("Using {} server: {}", server, ms_host);

    // The client's own path: ask a route host for the route list, and nothing
    // else. Three fewer requests than what this did, and — more to the point —
    // three fewer requests that identify the caller as not a browser.
    if server == "cn" {
        for host in CN_ROUTE_HOSTS {
            match fetch_json_cached::<RoutesResponse>(
                client,
                &routes_url(host, package_version),
                cache_dir,
                false,
            )
            .await
            {
                Ok(response) => {
                    if let Some(route) = response.data.routes.first() {
                        let endpoint = format!("wss://{}/gateway", route.domain);
                        info!(
                            "Discovered gateway: {} (route_id: {}, via {host})",
                            endpoint, route.id
                        );
                        // The resource version is not learned on this path and
                        // is not needed: its only caller ignores it, and the
                        // login carries the pinned code version instead.
                        return Ok((endpoint, String::new(), route.id.clone()));
                    }
                    warn!("{host} answered with no routes");
                }
                Err(error) => warn!("{host} did not answer routes ({error:#})"),
            }
        }
        warn!("no compiled-in route host answered; falling back to config.json discovery");
    }

    discover_gateway_via_config(client, server, cache_dir, package_version).await
}

/// The long way round: `version.json` → `resversion` → `config.json` → routes.
///
/// Kept only as a fallback for when every compiled-in route host is
/// unreachable, and for the non-CN servers this has never had a captured client
/// for. It is three requests a browser does not make, so it is not the path to
/// be on — but a deployment that cannot dial at all is worse than one that is
/// identifiable, and the hosts above are a static list that Mahjong Soul could
/// move.
async fn discover_gateway_via_config(
    client: &reqwest::Client,
    server: &str,
    cache_dir: &Path,
    package_version: &str,
) -> Result<(String, String, String)> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);

    let version_url = format!("{ms_host}{prefix}/version.json");
    let version_info: VersionInfo = fetch_json_cached(client, &version_url, cache_dir, true)
        .await
        .context("Failed to fetch version.json")?;

    let version = version_info.version.clone();
    info!("Majsoul version: {}", version);

    let config_url = match resolve_config_url(client, ms_host, prefix, &version, cache_dir).await {
        Ok(url) => url,
        Err(error) => {
            warn!(
                "resversion manifest unavailable ({error:#}); falling back to v{version}/config.json"
            );
            format!("{ms_host}{prefix}/v{version}/config.json")
        }
    };
    let config: Config = fetch_json_cached(client, &config_url, cache_dir, false)
        .await
        .context("Failed to fetch config.json")?;

    let gateway = config
        .ip
        .iter()
        .find(|entry| entry.name == "player")
        .or_else(|| config.ip.first())
        .and_then(|ip| ip.gateways.first())
        .context("No gateway found in config")?;

    let gateway_base = gateway.url.trim_end_matches('/');
    debug!("Gateway base URL: {}", gateway_base);

    let routes_response: RoutesResponse = fetch_json_cached(
        client,
        &routes_url(gateway_base, package_version),
        cache_dir,
        false,
    )
    .await
    .context("Failed to fetch routes")?;

    let route = routes_response
        .data
        .routes
        .first()
        .context("No routes found in response")?;

    let endpoint = format!("wss://{}/gateway", route.domain);
    info!("Discovered gateway: {} (route_id: {})", endpoint, route.id);

    Ok((endpoint, version, route.id.clone()))
}

/// Extract the Unity build version (`productVersion`, e.g. "4.0.45") from the
/// game's index.html. This is the login `client_version.package`; it changes
/// only on Unity rebuilds. Cache-backed with a fallback to the last good
/// value, then the caller's pinned default.
pub async fn discover_package_version(
    client: &reqwest::Client,
    server: &str,
    cache_dir: &Path,
) -> Result<String> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);
    let index_url = format!("{ms_host}{prefix}/");
    let cache = cache_dir.join("package_version");
    let pattern = Regex::new(r#"productVersion:\s*"([0-9]+\.[0-9]+\.[0-9]+)""#)?;

    if let Ok(html) = fetch_text_cached(client, &index_url, cache_dir, true, REQUEST_TIMEOUT).await
    {
        if let Some(caps) = pattern.captures(&html) {
            let version = caps[1].to_string();
            write_cache_atomic(&cache, &version);
            return Ok(version);
        }
    }

    if let Ok(cached) = std::fs::read_to_string(&cache) {
        let cached = cached.trim().to_string();
        if !cached.is_empty() {
            warn!("Using cached package version {cached} after index.html lookup failed");
            return Ok(cached);
        }
    }
    anyhow::bail!("could not determine package version from index.html")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_path(dir: &Path, url: &str) -> std::path::PathBuf {
        dir.join(format!("{:x}", Sha256::digest(url.as_bytes())))
    }

    /// The routes URL, character for character as the client asks for it.
    ///
    /// Taken from `captures/register_20260805_174145.json`, where a real browser
    /// asks all five route hosts for exactly this. Two things this used to get
    /// wrong are both in here: the `version` is the package version and not the
    /// resource version, and `lang` is present.
    #[test]
    fn the_routes_url_is_the_one_the_client_asks_for() {
        assert_eq!(
            routes_url("https://route-5.maj-soul.com", "4.0.45"),
            "https://route-5.maj-soul.com/api/clientgate/routes\
             ?platform=Web&version=4.0.45&lang=chs_t"
        );
        // Every compiled-in host is one the captures show being asked, port and
        // all — route-3 is the odd one on 8443.
        assert!(CN_ROUTE_HOSTS.contains(&"https://route-3.maj-soul.com:8443"));
        assert_eq!(CN_ROUTE_HOSTS.len(), 5);
        for host in CN_ROUTE_HOSTS {
            assert!(host.starts_with("https://route-"), "odd route host {host}");
        }
    }

    #[test]
    fn write_cache_atomic_creates_dirs_and_overwrites() {
        let dir = std::env::temp_dir().join(format!("mjai-gw-{}", Uuid::new_v4().simple()));
        let file = dir.join("nested/entry");
        write_cache_atomic(&file, "first");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "first");
        write_cache_atomic(&file, "second");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "second");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fetch_text_cached_falls_back_to_cache_on_failure() {
        let dir = std::env::temp_dir().join(format!("mjai-gw-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        // Port 9 (discard) refuses immediately, so the fetch fails fast and
        // the pre-seeded cache must be returned.
        let url = "http://127.0.0.1:9/version.json";
        std::fs::write(cache_path(&dir, url), "cached-body").unwrap();
        let client = reqwest::Client::new();
        let result = fetch_text_cached(&client, url, &dir, false, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(result, "cached-body");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fetch_text_cached_errors_when_no_cache_exists() {
        let dir = std::env::temp_dir().join(format!("mjai-gw-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let client = reqwest::Client::new();
        let result = fetch_text_cached(
            &client,
            "http://127.0.0.1:9/missing.json",
            &dir,
            false,
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
