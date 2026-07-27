export type StorageLocation = {
  pack_key: string;
  offset: number;
  compressed_size: number;
  raw_size: number;
  codec: "zstd";
};

export type MjaiRecord = {
  id: string;
  source: string;
  sha256: string;
  received_at: string;
  played_at: string | null;
  players: string[];
  rule: string | null;
  event_count: number;
  storage: StorageLocation;
};

export type RecordPage = {
  items: MjaiRecord[];
  next_cursor: string | null;
};

export type RecordFilters = {
  source?: string;
  player?: string;
  received_from?: string;
  received_to?: string;
  played_from?: string;
  played_to?: string;
  cursor?: string;
  limit?: number;
};

export type WatchUuidState =
  | "live"
  | "pending"
  | "fetching"
  | "fetched"
  | "failed";

export type WatchConversionState =
  | "waiting"
  | "converting"
  | "completed"
  | "failed";

export type WatchRecord = {
  uuid: string;
  uuid_state: WatchUuidState;
  conversion_state: WatchConversionState;
  mode_id: number | null;
  started_at: string | null;
  updated_at: string;
  attempts: number;
  message: string | null;
  record_id: string | null;
};

export type WatchSummary = {
  total: number;
  live: number;
  pending: number;
  converting: number;
  completed: number;
  failed: number;
  items: WatchRecord[];
  service: WatchRuntimeStatus;
};

export type WatchRuntimeStatus = {
  phase: "stopped" | "starting" | "running" | "reloading" | "stopping" | "failed";
  active_revision: number | null;
  login_module: WatchModuleRef | null;
  pb_fetch_module: WatchModuleRef | null;
  started_at: string | null;
  updated_at: string;
  last_error: string | null;
};

export type WatchModuleRef = {
  name: string;
  version: string;
};

export type WatchLogLevel = "debug" | "info" | "warn" | "error";

export type WatchLogEntry = {
  seq: number;
  timestamp: string;
  level: WatchLogLevel;
  source: string;
  message: string;
};

export type WatchLogPage = {
  boot_id: string;
  items: WatchLogEntry[];
  next_cursor: number | null;
};

/** One collector: an account watching one room and player count. */
export type WatchInstance = {
  id: string;
  enabled: boolean;
  room: "gold" | "jade" | "throne" | "all";
  players: 3 | 4;
  modes: Array<"east" | "south">;
  account_secret_ref: string;
  client_version: string | null;
};

export type WatchServiceConfig = {
  revision: number;
  enabled: boolean;
  server: "cn" | "en" | "jp";
  proxy_mode: "direct" | "mihomo" | "custom";
  custom_proxy_url: string | null;
  poll_interval_secs: number;
  request_delay_ms: number;
  login_module: WatchModuleRef;
  pb_fetch_module: WatchModuleRef;
  instances: WatchInstance[];
};

export type InstalledWatchModule = {
  kind: "login" | "pb_fetch";
  name: string;
  version: string;
  protocol_version: number;
  builtin: boolean;
  active: boolean;
};

export type MihomoNode = {
  name: string;
  node_type: string;
  alive: boolean | null;
  delay_ms: number | null;
  selected: boolean;
};

export type MihomoStatus = {
  available: boolean;
  subscription_configured: boolean;
  subscription_host: string | null;
  update_interval_secs: number;
  selected_node: string | null;
  proxy_url: string;
  nodes: MihomoNode[];
  updated_at: string;
  error: string | null;
};

export function buildRecordSearch(filters: RecordFilters): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  }
  return `/api/v1/records?${query.toString()}`;
}
