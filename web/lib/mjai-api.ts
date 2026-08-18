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
 * One Mahjong Soul account the console manages. `password` is never returned in
 * full — a stored one reads back as `"***"`, and handing that back on save keeps
 * it, so editing any other field does not wipe it.
 */
export type StoredAccount = {
  /** Assigned by the backend. Carries the password across a rename. */
  id: string;
  username: string;
  password: string;
  /** Which half may log in with it. One account cannot serve both. */
  purpose: "watch" | "refetch";
  note: string;
  enabled: boolean;
  /**
   * Which mihomo node this account's session goes out of, by name. Empty means
   * "whatever 批量补抓 is on", which is what every account did before the pool
   * could be spread over several exits.
   */
  node: string;
};

export type AccountDocument = {
  revision: number;
  accounts: StoredAccount[];
  updated_at: string | null;
};

/**
 * What the last login with an account did.
 *
 * Not a boolean, because "failed" covers three things an operator acts on
 * differently: `banned` is an account Mahjong Soul will never let in again and
 * has to be replaced; `refused` is an answer that often says nothing bad about
 * the account at all (`1005 ERR_ACC_ALREADY_LOGIN` means it is logged in
 * elsewhere, `151 ERR_CLIENT_VERSION` is this client's own fault); and
 * `unreachable` is the gateway or the proxy. Only the first is worth acting on.
 *
 * `unknown` means nothing has tried since the backend started — deliberately its
 * own state, because a fresh deployment showing every account green would be
 * claiming something it has not checked.
 */
export type AccountState =
  | "unknown"
  | "ok"
  | "banned"
  | "refused"
  | "unreachable";

export type AccountHealth = {
  state: AccountState;
  /** When that happened, or `null` for an account nothing has tried. */
  at: string | null;
  /** The error as the backend reported it. Empty when `ok`. */
  detail: string;
  /** Consecutive failures, cleared by a success. */
  failures: number;
};

/** Keyed by username, as the account document spells it. */
export type AccountHealthMap = Record<string, AccountHealth>;

/** One account a registration run tried to create. */
export type AccountRegisterOutcome = {
  /** The address only. The mailbox credential never leaves the backend. */
  email: string;
  ok: boolean;
  account_id: number | null;
  nickname: string | null;
  /** Which step failed. Empty on success. */
  stage: string;
  detail: string;
  at: string;
};

/**
 * Where a registration run has got to.
 *
 * No passwords: a created account goes straight into the pool, disabled, and
 * that is the only place its password exists. This is polled every few seconds
 * while a run is going, which is not somewhere credentials should live.
 */
export type AccountRegisterProgress = {
  running: boolean;
  total: number;
  done: number;
  succeeded: number;
  failed: number;
  /** The address in flight, so a run inside a mailbox poll does not look stuck. */
  current: string | null;
  started_at: string | null;
  finished_at: string | null;
  message: string | null;
  outcomes: AccountRegisterOutcome[];
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
  /**
   * Where the walk gets its uuids. `missing_pb` repairs rows already in the
   * index and finishes; `known_games` sweeps the resolved uuid list and does not.
   */
  work: "missing_pb" | "known_games";
  /**
   * Where the 牌谱屋 walk starts when it has no stored position. Only a seed —
   * once the walk has a cursor this is ignored.
   */
  sweep_from: string | null;
  server: "cn" | "en" | "jp";
  proxy_mode: "direct" | "mihomo" | "custom";
  custom_proxy_url: string | null;
  /** `pool:refetch`, or `file:`/`env:` naming one `username,password` per line. */
  account_secret_ref: string;
  concurrency: number;
  request_delay_ms: number;
  client_version: string | null;
};

export type RefetchProgress = {
  pass: number;
  scanned: number;
  replaced: number;
  /** 牌谱屋 walk: catalogued games recognised as already held, before any request. */
  present: number;
  /** 牌谱屋 walk: games fetched that turned out to be held after all. */
  duplicates: number;
  /**
   * 牌谱屋 walk: how far into the catalogue it has read. The only honest
   * measure of progress over half a billion games — there is no percentage to
   * quote, because the denominator is not what this run set out to fetch.
   */
  position: string | null;
  refused: number;
  unreadable: number;
  unconvertible: number;
  /** `unconvertible` split by cause — one number cannot tell the guard working from the converter breaking. */
  unconvertible_by: {
    no_uuid: number;
    convert_failed: number;
    wrong_game: number;
    truncated: number;
  };
};

export type UnconvertibleReason =
  | "no_uuid"
  | "convert_failed"
  | "wrong_game"
  | "truncated";

export type RefetchFailure = {
  at: string;
  /** Game uuid when known, record id when not. */
  subject: string;
  why: UnconvertibleReason;
  label: string;
  detail: string;
};

export type RefetchStatus = {
  phase: WatchRuntimeStatus["phase"];
  active_revision: number | null;
  /** What the run in progress is doing, as opposed to what the form says. */
  active_work: RefetchServiceConfig["work"] | null;
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
  /** Records per second over the last 30s. The counters say how much is done; this says whether anything is happening now. */
  qps: number;
  /** Most recent rejections, newest first. Bounded display buffer. */
  failures: RefetchFailure[];
};

/**
 * How much of what 牌谱屋 lists this corpus is missing.
 *
 * `listed`/`missing` are for the requested window; `games`/`earliest`/`latest`
 * describe the whole synced catalogue. Matching is on start time and the set of
 * player names, so a game counts as present without anyone having compared
 * uuids — which is the point: a uuid is only worth fetching for the ones that
 * do not match.
 */
export type PaipuyaReport = {
  games: number;
  earliest: string | null;
  latest: string | null;
  listed: number;
  missing: number;
};

/**
 * The 牌谱屋 sync. `api_key` is never returned in full: a stored key reads back
 * as `"***"`, and handing that back on save keeps the stored one — so editing
 * any other field does not wipe it.
 */
export type PaipuyaConfig = {
  revision: number;
  enabled: boolean;
  base_url: string;
  api_key: string | null;
  api_key_header: string;
  modes: number[];
  /** Left edge of the window. A cursor behind it is dragged up to it. */
  start_from: string;
  /** Right edge, or `null` for "keep up with live play forever". */
  sync_until: string | null;
  /** Floor on the gap between two requests, counting every worker together. */
  request_interval_ms: number;
  concurrency: number;
  page_size: number;
};

/** Where one mode's sweep stands, as PostgreSQL holds it between runs. */
export type PaipuyaModeProgress = {
  mode_id: number;
  /** One of the twelve rule tokens, for {@link ruleLabel}. */
  rule: string;
  next_from: string;
  synced_games: number;
  updated_at: string;
};

export type PaipuyaStatus = {
  phase: WatchRuntimeStatus["phase"];
  active_revision: number | null;
  updated_at: string;
  last_error: string | null;
  has_key: boolean;
  progress: {
    synced: number;
    without_uuid: number;
    requests: number;
    refused: number;
    cursors: Record<string, string>;
  };
  /** The window being swept; `window_end` null means "up to now". */
  window_start: string;
  window_end: string | null;
  /** The backend's clock at the read, which is the right edge of an open window. */
  now: string;
  modes: PaipuyaModeProgress[];
};

export type InstalledWatchModule = {
  kind: "login" | "pb_fetch";
  name: string;
  version: string;
  protocol_version: number;
  builtin: boolean;
  active: boolean;
};

/** Which half of the deployment an outbound path belongs to. */
export type MihomoLane = "watch" | "refetch";

/**
 * One half's own exit. `available` is false when mihomo does not have the
 * group — it kept the configuration it had — in which case the picker would
 * change nothing and says so instead.
 */
export type MihomoLaneStatus = {
  lane: MihomoLane;
  group: string;
  proxy_url: string;
  /** Verbatim. `"MAJSOUL"` means "follow whatever the deployment is on". */
  selected_node: string | null;
  /** What that resolves to — the shared group's node when following it. */
  effective_node: string | null;
  follows_shared: boolean;
  available: boolean;
};

/**
 * One node the re-fetch pool goes out of on its own listener, because at least
 * one account named it. `available` false means mihomo has not picked the group
 * up, and the accounts on it are going out of the 批量补抓 lane meanwhile.
 */
export type MihomoOutboundStatus = {
  node: string;
  group: string;
  proxy_url: string;
  selected_node: string | null;
  available: boolean;
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
  /** What the shared `MAJSOUL` group is on; both lanes follow it by default. */
  selected_node: string | null;
  lanes: MihomoLaneStatus[];
  /** One per node an account was bound to; empty when none is. */
  outbounds: MihomoOutboundStatus[];
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
