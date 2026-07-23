use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const PROVIDER_NAME: &str = "majsoul";
const GROUP_NAME: &str = "MAJSOUL";
const HEALTH_URL: &str = "https://www.gstatic.com/generate_204";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredSubscription {
    url: String,
    update_interval_secs: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SubscriptionUpdate {
    pub url: String,
    #[serde(default = "default_update_interval")]
    pub update_interval_secs: u64,
}

fn default_update_interval() -> u64 {
    3600
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProxySelection {
    pub name: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MihomoAction {
    RefreshSubscription,
    HealthCheck,
}

#[derive(Clone, Debug, Serialize)]
pub struct MihomoNode {
    pub name: String,
    pub node_type: String,
    pub alive: Option<bool>,
    pub delay_ms: Option<u64>,
    pub selected: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct MihomoStatus {
    pub available: bool,
    pub subscription_configured: bool,
    pub subscription_host: Option<String>,
    pub update_interval_secs: u64,
    pub selected_node: Option<String>,
    pub proxy_url: String,
    pub nodes: Vec<MihomoNode>,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum MihomoError {
    #[error("invalid mihomo configuration: {0}")]
    InvalidConfig(String),
    #[error("mihomo controller error: {0}")]
    Controller(String),
    #[error("mihomo IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("mihomo serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("mihomo HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

pub struct MihomoManager {
    root: PathBuf,
    controller_url: Url,
    controller_secret: String,
    proxy_url: String,
    subscription: RwLock<Option<StoredSubscription>>,
    client: Client,
}

impl MihomoManager {
    pub fn new(
        root: PathBuf,
        controller_url: &str,
        controller_secret: String,
        proxy_url: String,
    ) -> Result<Self, MihomoError> {
        std::fs::create_dir_all(root.join("providers"))?;
        let subscription_path = root.join("subscription.json");
        let subscription = match std::fs::read(&subscription_path) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let controller_url = Url::parse(controller_url)
            .map_err(|error| MihomoError::InvalidConfig(error.to_string()))?;
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(15))
            .build()?;
        let manager = Self {
            root,
            controller_url,
            controller_secret,
            proxy_url,
            subscription: RwLock::new(subscription),
            client,
        };
        manager.write_runtime_config()?;
        Ok(manager)
    }

    pub fn proxy_url(&self) -> &str {
        &self.proxy_url
    }

    pub async fn status(&self) -> MihomoStatus {
        match self.read_nodes().await {
            Ok((selected_node, nodes)) => self.status_value(true, selected_node, nodes, None),
            Err(error) => self.status_value(false, None, Vec::new(), Some(error.to_string())),
        }
    }

    fn status_value(
        &self,
        available: bool,
        selected_node: Option<String>,
        nodes: Vec<MihomoNode>,
        error: Option<String>,
    ) -> MihomoStatus {
        let subscription = self.subscription.read().clone();
        MihomoStatus {
            available,
            subscription_configured: subscription.is_some(),
            subscription_host: subscription
                .as_ref()
                .and_then(|value| redacted_host(&value.url)),
            update_interval_secs: subscription
                .map(|value| value.update_interval_secs)
                .unwrap_or(default_update_interval()),
            selected_node,
            proxy_url: self.proxy_url.clone(),
            nodes,
            updated_at: Utc::now(),
            error,
        }
    }

    pub async fn update_subscription(
        &self,
        update: SubscriptionUpdate,
    ) -> Result<MihomoStatus, MihomoError> {
        validate_subscription(&update)?;
        let stored = StoredSubscription {
            url: update.url,
            update_interval_secs: update.update_interval_secs,
        };
        persist_secret_json(&self.root.join("subscription.json"), &stored)?;

        let previous = self.subscription.read().clone();
        *self.subscription.write() = Some(stored);
        if let Err(error) = self.write_runtime_config() {
            *self.subscription.write() = previous;
            return Err(error);
        }
        self.reload_config().await?;
        self.refresh_subscription().await?;
        Ok(self.status().await)
    }

    pub async fn select(&self, selection: ProxySelection) -> Result<MihomoStatus, MihomoError> {
        if selection.name.is_empty()
            || selection.name.len() > 256
            || selection.name.chars().any(char::is_control)
        {
            return Err(MihomoError::InvalidConfig(
                "proxy node name is invalid".into(),
            ));
        }
        self.controller_json(
            Method::PUT,
            &format!("/proxies/{GROUP_NAME}"),
            Some(serde_json::json!({"name": selection.name})),
        )
        .await?;
        Ok(self.status().await)
    }

    pub async fn action(&self, action: MihomoAction) -> Result<MihomoStatus, MihomoError> {
        match action {
            MihomoAction::RefreshSubscription => self.refresh_subscription().await?,
            MihomoAction::HealthCheck => {
                self.controller_json(
                    Method::GET,
                    &format!("/providers/proxies/{PROVIDER_NAME}/healthcheck"),
                    None,
                )
                .await?;
            }
        }
        Ok(self.status().await)
    }

    async fn refresh_subscription(&self) -> Result<(), MihomoError> {
        if self.subscription.read().is_none() {
            return Err(MihomoError::InvalidConfig(
                "configure a subscription before refreshing it".into(),
            ));
        }
        self.controller_json(
            Method::PUT,
            &format!("/providers/proxies/{PROVIDER_NAME}"),
            Some(serde_json::json!({})),
        )
        .await?;
        Ok(())
    }

    async fn reload_config(&self) -> Result<(), MihomoError> {
        self.controller_json(
            Method::PUT,
            "/configs?force=true",
            Some(serde_json::json!({
                "path": "/root/.config/mihomo/config.yaml"
            })),
        )
        .await?;
        Ok(())
    }

    async fn read_nodes(&self) -> Result<(Option<String>, Vec<MihomoNode>), MihomoError> {
        let value = self.controller_json(Method::GET, "/proxies", None).await?;
        let proxies = value
            .get("proxies")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| MihomoError::Controller("missing proxies object".into()))?;
        let selected = proxies
            .get(GROUP_NAME)
            .and_then(|group| group.get("now"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let provider_names = self.provider_node_names().await.unwrap_or_default();
        let mut nodes = Vec::new();
        for name in provider_names {
            let Some(node) = proxies.get(&name) else {
                continue;
            };
            let delay_ms = node
                .get("history")
                .and_then(serde_json::Value::as_array)
                .and_then(|history| history.last())
                .and_then(|sample| sample.get("delay"))
                .and_then(serde_json::Value::as_u64);
            nodes.push(MihomoNode {
                selected: selected.as_deref() == Some(name.as_str()),
                name,
                node_type: node
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Unknown")
                    .to_owned(),
                alive: node.get("alive").and_then(serde_json::Value::as_bool),
                delay_ms,
            });
        }
        nodes.sort_by(|left, right| {
            (
                !left.selected,
                left.delay_ms.unwrap_or(u64::MAX),
                &left.name,
            )
                .cmp(&(
                    !right.selected,
                    right.delay_ms.unwrap_or(u64::MAX),
                    &right.name,
                ))
        });
        Ok((selected, nodes))
    }

    async fn provider_node_names(&self) -> Result<Vec<String>, MihomoError> {
        let value = self
            .controller_json(
                Method::GET,
                &format!("/providers/proxies/{PROVIDER_NAME}"),
                None,
            )
            .await?;
        Ok(value
            .get("proxies")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|node| node.get("name").and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .collect())
    }

    async fn controller_json(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, MihomoError> {
        let url = self
            .controller_url
            .join(path.trim_start_matches('/'))
            .map_err(|error| MihomoError::InvalidConfig(error.to_string()))?;
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(&self.controller_secret);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            return Err(MihomoError::Controller(format!(
                "{status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        if bytes.is_empty() || status == StatusCode::NO_CONTENT {
            Ok(serde_json::json!({}))
        } else {
            serde_json::from_slice(&bytes).map_err(MihomoError::Json)
        }
    }

    fn write_runtime_config(&self) -> Result<(), MihomoError> {
        let subscription = self.subscription.read().clone();
        let provider = subscription.as_ref().map(|value| {
            format!(
                r#"
proxy-providers:
  {PROVIDER_NAME}:
    type: http
    url: {}
    path: ./providers/majsoul.yaml
    interval: {}
    health-check:
      enable: true
      url: {HEALTH_URL}
      interval: 300
      timeout: 5000
      lazy: true
      expected-status: 204
"#,
                serde_json::to_string(&value.url).expect("URL can be JSON encoded"),
                value.update_interval_secs,
            )
        });
        let provider_use = if subscription.is_some() {
            format!("    use:\n      - {PROVIDER_NAME}\n")
        } else {
            String::new()
        };
        let config = format!(
            r#"mixed-port: 7890
allow-lan: true
bind-address: "*"
mode: rule
log-level: info
ipv6: false
external-controller: 0.0.0.0:9090
secret: {}
{}
proxy-groups:
  - name: {GROUP_NAME}
    type: select
    proxies:
      - DIRECT
{}rules:
  - MATCH,{GROUP_NAME}
"#,
            serde_json::to_string(&self.controller_secret)
                .expect("controller secret can be JSON encoded"),
            provider.unwrap_or_default(),
            provider_use,
        );
        atomic_write(&self.root.join("config.yaml"), config.as_bytes())
    }
}

fn validate_subscription(update: &SubscriptionUpdate) -> Result<(), MihomoError> {
    let url =
        Url::parse(&update.url).map_err(|error| MihomoError::InvalidConfig(error.to_string()))?;
    if !matches!(url.scheme(), "https" | "http") || url.host_str().is_none() {
        return Err(MihomoError::InvalidConfig(
            "subscription URL must be HTTP or HTTPS".into(),
        ));
    }
    if !(300..=604_800).contains(&update.update_interval_secs) {
        return Err(MihomoError::InvalidConfig(
            "update interval must be between 300 and 604800 seconds".into(),
        ));
    }
    Ok(())
}

fn redacted_host(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

fn persist_secret_json<T: Serialize>(path: &Path, value: &T) -> Result<(), MihomoError> {
    let mut body = serde_json::to_vec_pretty(value)?;
    body.push(b'\n');
    atomic_write(path, &body)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), MihomoError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_returns_subscription_host() {
        assert_eq!(
            redacted_host("https://user:token@example.com:8443/sub?token=secret").as_deref(),
            Some("example.com:8443")
        );
    }

    #[test]
    fn rejects_non_http_subscription() {
        assert!(
            validate_subscription(&SubscriptionUpdate {
                url: "file:///tmp/sub".into(),
                update_interval_secs: 3600,
            })
            .is_err()
        );
    }
}
