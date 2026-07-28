import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** The house dropdown is a native select; components/ui/select.tsx is unused. */
export const selectClass =
  "h-9 w-full rounded-lg border border-input bg-background px-3 text-sm outline-none focus:border-ring focus:ring-3 focus:ring-ring/30"

const dateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  hour12: false,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
})

export function formatDateTime(value: string | null) {
  return value ? dateTimeFormatter.format(new Date(value)) : "—"
}

/**
 * The instants an `<input type="date">` day means to the operator reading the
 * table, which is the operator's own day and not UTC's. Timestamps render in
 * the browser's timezone, so pinning a day to `Z` shifts the whole window by
 * that offset: in UTC+8, picking 2026-07-27 would select 08:00 that morning
 * through 07:59 the next, silently dropping eight hours of rows the table had
 * just shown under that date and pulling in eight it had not — and the export
 * page stores the same bounds as a job's filter snapshot, so the archive
 * inherits the shift.
 *
 * The end is the next local midnight, not 23:59:59, because the backend
 * compares `received_at < to`; a second-precision end also excluded everything
 * in the final second of the day.
 */
export function dayStart(day: string) {
  return day ? new Date(`${day}T00:00:00`).toISOString() : ""
}

export function dayEnd(day: string) {
  if (!day) return ""
  const next = new Date(`${day}T00:00:00`)
  next.setDate(next.getDate() + 1)
  // Resolved back through `dayStart` rather than used directly, so that
  // `dayEnd(d)` and `dayStart(d + 1)` are the same instant by construction. In
  // the few zones whose DST jump lands on midnight, one of those midnights does
  // not exist and the runtime resolves it an hour away; two bounds computed
  // independently would then overlap, and that hour's records would come back
  // under both days — in the table, and in two separate export archives.
  return dayStart(
    `${next.getFullYear()}-${String(next.getMonth() + 1).padStart(2, "0")}-${String(next.getDate()).padStart(2, "0")}`,
  )
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

export function formatBytes(bytes: number) {
  let unit = 0
  let value = bytes
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024
    unit += 1
  }
  return `${value.toFixed(unit === 0 ? 0 : 2)} ${BYTE_UNITS[unit]}`
}
