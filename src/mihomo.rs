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
/// The original group, kept exactly as it was. Nothing selects it any more —
/// both halves have their own — but it is what `mixed-port: 7890` and the rule
/// still point at, and 7890 is the address every deployment before this one
/// dials. Removing it would move the outbound path of a live collector as a
/// side effect of an upgrade.
const GROUP_NAME: &str = "MAJSOUL";
const HEALTH_URL: &str = "https://www.gstatic.com/generate_204";

/// Which half of the deployment a request belongs to.
///
/// Live collection and the re-fetch pool go out of separate listeners bound to
/// separate select groups, so an operator can put them on different nodes. That
/// is not cosmetic: they are different Mahjong Soul accounts doing visibly
/// different things from one address, and the pool's traffic is the half that
/// looks like a script.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MihomoLane {
    Watch,
    Refetch,
}

impl MihomoLane {
    /// The select group this lane's listener is bound to.
    fn group(self) -> &'static str {
        match self {
            Self::Watch => "MAJSOUL-WATCH",
            Self::Refetch => "MAJSOUL-REFETCH",
        }
    }

    fn port(self) -> u16 {
        match self {
            Self::Watch => 7891,
            Self::Refetch => 7892,
        }
    }

    fn listener(self) -> &'static str {
        match self {
            Self::Watch => "majsoul-watch-in",
            Self::Refetch => "majsoul-refetch-in",
        }
    }

    pub const ALL: [Self; 2] = [Self::Watch, Self::Refetch];
}

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
    /// Which half to move. Defaulted to live collection so a console built
    /// before the split still selects something rather than failing to parse.
    #[serde(default = "default_lane")]
    pub lane: MihomoLane,
}

fn default_lane() -> MihomoLane {
    MihomoLane::Watch
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MihomoAction {
    RefreshSubscription,
    HealthCheck,
}

/// One lane's own selection, and whether mihomo actually has its group.
///
/// `available` is the honest half. The two groups and their listeners are
/// written into a configuration this process generates, and if mihomo will not
/// accept them it keeps running on the old one — so the console has to be able
/// to say "this lane is not there" rather than show a picker that silently
/// changes nothing.
#[derive(Clone, Debug, Serialize)]
pub struct MihomoLaneStatus {
    pub lane: MihomoLane,
    pub group: String,
    pub proxy_url: String,
    pub selected_node: Option<String>,
    pub available: bool,
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
    /// What the live-collection lane is on. Kept under its old name so a
    /// console that has not been updated still shows something true.
    pub selected_node: Option<String>,
    /// One entry per lane, keyed by its `snake_case` name.
    pub lanes: Vec<MihomoLaneStatus>,
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

    /// Where one half of the deployment dials.
    ///
    /// Derived from the shared proxy URL's host so a deployment that moved
    /// mihomo somewhere else does not have to say so three times. Falls back to
    /// the shared port if that URL cannot be parsed, which keeps a malformed
    /// setting costing the split rather than costing all outbound traffic.
    pub fn proxy_url_for(&self, lane: MihomoLane) -> String {
        let with_port = || {
            let mut url = Url::parse(&self.proxy_url).ok()?;
            url.set_port(Some(lane.port())).ok()?;
            Some(url.to_string())
        };
        with_port().unwrap_or_else(|| self.proxy_url.clone())
    }

    pub async fn status(&self) -> MihomoStatus {
        match self.read_nodes().await {
            Ok((lanes, nodes)) => self.status_value(true, lanes, nodes, None),
            Err(error) => self.status_value(false, Vec::new(), Vec::new(), Some(error.to_string())),
        }
    }

    fn status_value(
        &self,
        available: bool,
        lanes: Vec<MihomoLaneStatus>,
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
            selected_node: lanes
                .iter()
                .find(|lane| lane.lane == MihomoLane::Watch)
                .and_then(|lane| lane.selected_node.clone()),
            lanes,
            proxy_url: self.proxy_url.clone(),
            nodes,
            updated_at: Utc::now(),
            error,
        }
    }

    /// Makes mihomo read the configuration this process just wrote.
    ///
    /// Called once behind the listener at boot, because the file is generated
    /// here and mihomo only reads it when it starts or when it is told to. An
    /// upgrade that adds a group — which is what the per-half split is — would
    /// otherwise not take effect until somebody restarted the container, and
    /// the console would show two lanes that are not there. Failure is reported
    /// and swallowed: mihomo may simply not be up yet, and the deployment runs
    /// on the configuration it already has either way.
    pub async fn apply_runtime_config(&self) {
        match self.reload_config().await {
            Ok(()) => tracing::info!("mihomo 已重新读取本进程生成的配置"),
            Err(error) => tracing::warn!(%error, "mihomo 没有重新读取配置，出站分组可能还是旧的"),
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
            &format!("/proxies/{}", selection.lane.group()),
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

    async fn read_nodes(&self) -> Result<(Vec<MihomoLaneStatus>, Vec<MihomoNode>), MihomoError> {
        let value = self.controller_json(Method::GET, "/proxies", None).await?;
        let proxies = value
            .get("proxies")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| MihomoError::Controller("missing proxies object".into()))?;
        // Read from mihomo rather than from what was written: this process
        // generates the configuration but mihomo decides whether to accept it,
        // and a lane whose group is absent is a lane whose picker would change
        // nothing.
        let lanes: Vec<MihomoLaneStatus> = MihomoLane::ALL
            .into_iter()
            .map(|lane| {
                let group = proxies.get(lane.group());
                MihomoLaneStatus {
                    lane,
                    group: lane.group().to_owned(),
                    proxy_url: self.proxy_url_for(lane),
                    selected_node: group
                        .and_then(|group| group.get("now"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    available: group.is_some(),
                }
            })
            .collect();
        let selected = lanes
            .iter()
            .find(|lane| lane.lane == MihomoLane::Watch)
            .and_then(|lane| lane.selected_node.clone());
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
        Ok((lanes, nodes))
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
        // One select group per lane, plus the original. Their listeners are
        // bound straight to a group, which bypasses `rules` entirely — that is
        // the whole mechanism: the rule below can only name one group, so
        // routing by port is the only way two halves of one process reach two
        // different nodes.
        //
        // 7890 and `MAJSOUL` are unchanged and still ruled to. Every deployment
        // before this one dials that port, and an upgrade must not move a
        // running collector's exit as a side effect. The two new ports are
        // additive; if mihomo rejects them it keeps the configuration it has,
        // which is why `MihomoLaneStatus::available` is read back from the
        // controller rather than assumed.
        let lane_groups: String = MihomoLane::ALL
            .into_iter()
            .map(|lane| {
                format!(
                    "  - name: {}\n    type: select\n    proxies:\n      - DIRECT\n{provider_use}",
                    lane.group()
                )
            })
            .collect();
        let listeners: String = MihomoLane::ALL
            .into_iter()
            .map(|lane| {
                format!(
                    "  - name: {}\n    type: mixed\n    port: {}\n    listen: 0.0.0.0\n    proxy: {}\n",
                    lane.listener(),
                    lane.port(),
                    lane.group()
                )
            })
            .collect();
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
{}{lane_groups}listeners:
{listeners}rules:
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

#[cfg(test)]
mod lane_tests {
    use super::*;

    /// The generated configuration, checked for the things that decide whether
    /// the split works at all.
    ///
    /// The suite cannot start mihomo, so this asserts the shape. The behaviour
    /// behind the shape was checked once, by hand, against `metacubex/mihomo`
    /// v1.19.27 — the version the deployment runs — and is worth writing down
    /// because it is what the whole feature rests on: a `listeners` entry with a
    /// `proxy:` bypasses `rules` entirely, which is the only way two halves of
    /// one process reach two different nodes when a rule can name one group.
    /// With the two lanes put on DIRECT and REJECT, a request through 7891
    /// answered 204 and the same request through 7892 answered 502, and
    /// selecting on one group left the other and `MAJSOUL` where they were.
    #[test]
    fn each_lane_gets_its_own_group_and_its_own_listener() {
        let root = std::env::temp_dir().join(format!("mjai-mihomo-{}", uuid::Uuid::new_v4()));
        // With a subscription, so the `use:` block each lane group needs is
        // actually interpolated rather than left out by an empty one.
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("subscription.json"),
            br#"{"url":"https://provider.example/sub?token=x","update_interval_secs":3600}"#,
        )
        .unwrap();
        let manager = MihomoManager::new(
            root.clone(),
            "http://127.0.0.1:9090",
            "secret".into(),
            "http://mihomo:7890".into(),
        )
        .expect("a manager with no subscription");
        let config = std::fs::read_to_string(root.join("config.yaml")).expect("a written config");

        // The path every existing deployment dials, untouched: an upgrade must
        // not move a running collector's exit as a side effect.
        assert!(config.contains("mixed-port: 7890"), "{config}");
        assert!(config.contains("  - MATCH,MAJSOUL\n"), "{config}");

        for lane in MihomoLane::ALL {
            assert!(
                config.contains(&format!("  - name: {}\n    type: select", lane.group())),
                "{lane:?} has no group: {config}"
            );
            assert!(
                config.contains(&format!("    port: {}\n", lane.port())),
                "{lane:?} has no listener: {config}"
            );
            assert!(
                config.contains(&format!("    proxy: {}\n", lane.group())),
                "{lane:?}'s listener is not bound to its group: {config}"
            );
            // And the URL each half dials is that listener, on the host the
            // shared setting names.
            assert_eq!(
                manager.proxy_url_for(lane),
                format!("http://mihomo:{}/", lane.port())
            );
        }
        std::fs::remove_dir_all(root).ok();
    }
}
