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
const CODE_JS_TIMEOUT: Duration = Duration::from_secs(60);
// Last-resort client version when neither the live assets nor the on-disk
// cache yield one. It goes stale over time, so it is only a floor: the
// resversion-driven fetch and cache above it keep the value current.
const KNOWN_CLIENT_VERSION: &str = "WebGL_2022-0.16.251";

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
pub async fn discover_gateway(
    client: &reqwest::Client,
    server: &str,
    cache_dir: &Path,
) -> Result<(String, String, String)> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);
    info!("Using {} server: {}", server, ms_host);

    // Step 1: Get version (always cache-busted so a new release is picked up
    // immediately, matching the reference client).
    let version_url = format!("{ms_host}{prefix}/version.json");
    let version_info: VersionInfo = fetch_json_cached(client, &version_url, cache_dir, true)
        .await
        .context("Failed to fetch version.json")?;

    let version = version_info.version.clone();
    let version_clean = version.replace(".w", "");
    info!("Majsoul version: {}", version);

    // Step 2: Resolve config.json through the resversion manifest. Fall back
    // to the legacy v{version} path if the manifest cannot be read so the
    // behaviour never regresses relative to the previous implementation.
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

    // Gateway URL is like "https://route-2.maj-soul.com". Match the official
    // client and prefer the "player" gateway list, falling back to the first
    // entry if the name is absent.
    let gateway = config
        .ip
        .iter()
        .find(|entry| entry.name == "player")
        .or_else(|| config.ip.first())
        .and_then(|ip| ip.gateways.first())
        .context("No gateway found in config")?;

    let gateway_base = gateway.url.trim_end_matches('/');
    debug!("Gateway base URL: {}", gateway_base);

    // Step 3: Fetch routes to get route_id (cache-busted; the active gateway
    // rotates and must not be served stale).
    let routes_url = format!(
        "{}/api/clientgate/routes?platform=Web&version={}",
        gateway_base, version
    );
    let routes_response: RoutesResponse = fetch_json_cached(client, &routes_url, cache_dir, true)
        .await
        .context("Failed to fetch routes")?;

    let route = routes_response
        .data
        .routes
        .first()
        .context("No routes found in response")?;

    let route_id = route.id.clone();
    let route_domain = &route.domain;
    debug!("Route: domain={}, id={}", route_domain, route_id);

    // Build WebSocket endpoint from route domain
    let endpoint = format!("wss://{}/gateway", route_domain);
    info!("Discovered gateway: {} (route_id: {})", endpoint, route_id);

    Ok((endpoint, version_clean, route_id))
}

/// Discover the client version string embedded in the current web client.
/// The resource version (`0.11.xxx.w`) is not the same value sent to
/// fetchGameRecord; current clients send strings such as
/// `WebGL_2022-0.16.251`.
///
/// The extracted value is cached in `cache_dir`; if the live code.js cannot be
/// fetched or parsed, the last successfully extracted value is reused before
/// falling back to the hard-coded floor. This keeps the version current
/// without a stale constant, matching the reference client's cache-as-fallback
/// behaviour.
pub async fn discover_client_version(
    client: &reqwest::Client,
    server: &str,
    resource_version: &str,
    cache_dir: &Path,
) -> Result<String> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);
    let pattern = Regex::new(r"WebGL_[0-9]{4}-[0-9]+\.[0-9]+\.[0-9]+")?;
    let version_cache = cache_dir.join("client_version");
    let mut versions = vec![resource_version.to_string()];
    if !resource_version.ends_with(".w") {
        versions.insert(0, format!("{}.w", resource_version));
    }

    let mut last_error = None;
    for version in versions {
        let code_url = format!("{ms_host}{prefix}/v{version}/code.js");
        let response = tokio::time::timeout(CODE_JS_TIMEOUT, async {
            client.get(&code_url).send().await?.error_for_status()
        })
        .await;
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                last_error = Some(format!("{}: {}", code_url, error));
                continue;
            }
            Err(error) => {
                last_error = Some(format!("{}: {}", code_url, error));
                continue;
            }
        };
        let code = match response.text().await {
            Ok(code) => code,
            Err(error) => {
                last_error = Some(format!("{}: {}", code_url, error));
                continue;
            }
        };
        if let Some(found) = pattern.find(&code) {
            let version = found.as_str().to_string();
            write_cache_atomic(&version_cache, &version);
            return Ok(version);
        }
        last_error = Some(format!("No WebGL client version found in {}", code_url));
    }

    if let Ok(cached) = std::fs::read_to_string(&version_cache) {
        let cached = cached.trim().to_string();
        if !cached.is_empty() {
            warn!(
                "Could not extract client_version_string from official assets ({}); using cached {}",
                last_error.unwrap_or_else(|| "no candidate resource version".to_string()),
                cached
            );
            return Ok(cached);
        }
    }

    warn!(
        "Could not extract client_version_string from official assets ({}) and no cache exists; using known compatible {}",
        last_error.unwrap_or_else(|| "no candidate resource version".to_string()),
        KNOWN_CLIENT_VERSION
    );
    Ok(KNOWN_CLIENT_VERSION.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_path(dir: &Path, url: &str) -> std::path::PathBuf {
        dir.join(format!("{:x}", Sha256::digest(url.as_bytes())))
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
