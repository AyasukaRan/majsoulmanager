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

export function buildRecordSearch(filters: RecordFilters): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  }
  return `/api/v1/records?${query.toString()}`;
}
