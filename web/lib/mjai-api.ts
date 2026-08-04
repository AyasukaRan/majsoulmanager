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

/** The seven fields an export job stores as its snapshot; timestamps are RFC 3339. */
export type RecordFilter = {
  source?: string;
  player?: string;
  rule?: string;
  received_from?: string;
  received_to?: string;
  played_from?: string;
  played_to?: string;
};

export type RecordFilters = RecordFilter & {
  cursor?: string;
  limit?: number;
};

export type JobState = "queued" | "running" | "completed" | "failed";

export type DownloadJob = {
  id: string;
  state: JobState;
  created_at: string;
  /** Only final once the job completes; before that it counts what has been written so far. */
  record_count: number;
  download_url: string | null;
  error: string | null;
};

/** Serde renames these variants, so the wire literals carry the file extension. */
export type DownloadFormat = "tar.gz" | "manifest.jsonl";

export type DownloadRequest = {
  filter: RecordFilter;
  format: DownloadFormat;
};

export type DownloadJobPage = {
  items: DownloadJob[];
};

export type SourceStat = {
  source: string;
  records: number;
};

export type StatsSummary = {
  records: {
    total: number;
    last_24h: number;
    sources: SourceStat[];
  };
  storage: {
    packs: number;
    raw_bytes: number;
    compressed_bytes: number;
  };
  downloads: Record<JobState, number>;
  watch: {
    phase: string;
    live: number;
    pending: number;
    completed: number;
    failed: number;
  };
};

/** How wide one point of a trend chart is. */
export type SeriesUnit = "hour" | "day";

/**
 * One bucket of the trend charts. `records` and the byte sums count what
 * arrived in it, `games` counts what was *played* in it — an import moves the
 * first three without moving the last.
 */
export type SeriesPoint = {
  /** `YYYY-MM-DD` for a day, RFC 3339 for an hour. UTC either way. */
  at: string;
  records: number;
  games: number;
  raw_bytes: number;
  compressed_bytes: number;
};

/** Games in the window by mode, busiest first; `rule` is "" where unknown. */
export type RuleCount = {
  rule: string;
  games: number;
};

export type Series = {
  unit: SeriesUnit;
  /** Gap-filled by the API: one entry per bucket in the window, oldest first. */
  points: SeriesPoint[];
  /** Counted over the same rows as `points[].games`, so the two agree. */
  rules: RuleCount[];
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
  /** Display label and log tag; safe to rename. */
  id: string;
  /**
   * Names this collector's state file on the backend, which is why it is never
   * edited or invented here: it is absent only on an instance this console has
   * just created, and the backend assigns one to exactly those.
   */
  key?: string;
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

/**
 * The re-fetch pool. It logs in with accounts of its own rather than borrowing a
 * collector's session, so `concurrency` is how many sessions run at once — and
 * it cannot exceed the number of accounts, because Mahjong Soul allows one
 * session per account. The backend reports what it settled on as
 * `RefetchStatus.workers`.
 */
export type RefetchServiceConfig = {
  revision: number;
  enabled: boolean;
  server: "cn" | "en" | "jp";
  proxy_mode: "direct" | "mihomo" | "custom";
  custom_proxy_url: string | null;
  /** `file:` or `env:`, one `username,password` per line. Never the accounts themselves. */
  account_secret_ref: string;
  concurrency: number;
  request_delay_ms: number;
  client_version: string | null;
};

export type RefetchProgress = {
  pass: number;
  scanned: number;
  replaced: number;
  refused: number;
  unreadable: number;
  unconvertible: number;
};

export type RefetchStatus = {
  phase: WatchRuntimeStatus["phase"];
  active_revision: number | null;
  started_at: string | null;
  updated_at: string;
  last_error: string | null;
  accounts: number;
  workers: number;
  sessions: number;
  waiting: number;
  /** Records with no protobuf when the run started; null before the first run. */
  backlog: number | null;
  progress: RefetchProgress;
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

/**
 * Every proxy route answers `{ error }` on failure, so the backend's own wording
 * — "received_at range must not exceed 90 days" and friends — is what reaches
 * the operator instead of a generic status code.
 */
export async function jsonRequest<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(path, {
    cache: "no-store",
    ...init,
    headers: {
      "content-type": "application/json",
      ...init?.headers,
    },
  });
  // Parsed by hand rather than with `response.json()` because a failure can also
  // come from a layer that answers no body at all — a proxy timeout, or a method
  // the backend does not route — and a JSON parse error there would hide the
  // status code that actually explains it.
  const raw = await response.text();
  let value: (T & { error?: string }) | null = null;
  try {
    value = raw ? (JSON.parse(raw) as T & { error?: string }) : null;
  } catch {
    value = null;
  }
  if (!response.ok || value === null) {
    throw new ApiRequestError(
      value?.error ?? `请求失败(${response.status})`,
      response.status,
    );
  }
  return value;
}

/**
 * Carries the status alongside the message, because some failures need a
 * different response and not just a different sentence: a 412 on a configuration
 * save means the document was edited elsewhere and this copy has to be rebuilt
 * on the current one, where every other failure means the edit in hand is still
 * the right one to retry. Callers that only render `error.message` are
 * unaffected.
 */
export class ApiRequestError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiRequestError";
  }
}

/**
 * Points at the browser-reachable proxy, not at the Rust path: the API key that
 * `/api/v1/records` needs only ever exists on the server side of that route.
 */
export function buildRecordSearch(filters: RecordFilters): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  }
  return `/api/records?${query.toString()}`;
}
