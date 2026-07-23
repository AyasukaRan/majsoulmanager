use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, info, warn};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
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

#[derive(Debug, Deserialize)]
struct Gateway {
    url: String,
}

#[derive(Debug, Deserialize)]
struct IpEntry {
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

/// Discover gateway endpoint, version, and route_id for Majsoul server
///
/// Returns (endpoint, version, route_id) tuple needed for connection
pub async fn discover_gateway(
    client: &reqwest::Client,
    server: &str,
) -> Result<(String, String, String)> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);
    info!("Using {} server: {}", server, ms_host);

    // Step 1: Get version
    let version_url = format!("{}{}/version.json", ms_host, prefix);
    let version_info: VersionInfo = tokio::time::timeout(REQUEST_TIMEOUT, async {
        client
            .get(&version_url)
            .send()
            .await?
            .json()
            .await
            .context("Failed to parse version.json")
    })
    .await
    .context("Timeout fetching version.json")??;

    let version = &version_info.version;
    let version_clean = version.replace(".w", "");
    info!("Majsoul version: {}", version);

    // Step 2: Get config to find gateway URL
    let config_url = format!("{}{}/v{}/config.json", ms_host, prefix, version);
    let config: Config = tokio::time::timeout(REQUEST_TIMEOUT, async {
        client
            .get(&config_url)
            .send()
            .await?
            .json()
            .await
            .context("Failed to parse config.json")
    })
    .await
    .context("Timeout fetching config.json")??;

    // Gateway URL is like "https://route-2.maj-soul.com"
    let gateway = config
        .ip
        .first()
        .and_then(|ip| ip.gateways.first())
        .context("No gateway found in config")?;

    let gateway_base = gateway.url.trim_end_matches('/');
    debug!("Gateway base URL: {}", gateway_base);

    // Step 3: Fetch routes to get route_id
    let routes_url = format!(
        "{}/api/clientgate/routes?platform=Web&version={}",
        gateway_base, version
    );
    let routes_response: RoutesResponse = tokio::time::timeout(REQUEST_TIMEOUT, async {
        client
            .get(&routes_url)
            .send()
            .await?
            .json()
            .await
            .context("Failed to parse routes response")
    })
    .await
    .context("Timeout fetching routes")??;

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
pub async fn discover_client_version(
    client: &reqwest::Client,
    server: &str,
    resource_version: &str,
) -> Result<String> {
    let ms_host = server_base_url(server);
    let prefix = server_path_prefix(server);
    let pattern = Regex::new(r"WebGL_[0-9]{4}-[0-9]+\.[0-9]+\.[0-9]+")?;
    let mut versions = vec![resource_version.to_string()];
    if !resource_version.ends_with(".w") {
        versions.insert(0, format!("{}.w", resource_version));
    }

    let mut last_error = None;
    for version in versions {
        let code_url = format!("{}{}/v{}/code.js", ms_host, prefix, version);
        let response = tokio::time::timeout(Duration::from_secs(60), async {
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
        let code = response.text().await?;
        if let Some(found) = pattern.find(&code) {
            return Ok(found.as_str().to_string());
        }
        last_error = Some(format!("No WebGL client version found in {}", code_url));
    }

    warn!(
        "Could not extract client_version_string from official assets ({}); using known compatible {}",
        last_error.unwrap_or_else(|| "no candidate resource version".to_string()),
        KNOWN_CLIENT_VERSION
    );
    Ok(KNOWN_CLIENT_VERSION.to_string())
}
