/**
 * Placing a 牌谱屋 sync cursor inside the window it is sweeping.
 *
 * Plain JavaScript so `node --test` can import it without a build step, the
 * same reason `accounts-jsonl.mjs` is.
 *
 * Time is the only honest measure here. How many games the catalogue holds for
 * a window is not known until the window has been swept — that is what the
 * sweep is for — so a percentage of games would be a percentage of a number
 * nobody has. The window is exact, and a cursor is a point in it.
 */

/** `2026-03-12`. A cursor at this scale has nothing to say about the clock. */
export function day(iso) {
  return typeof iso === "string" ? iso.slice(0, 10) : "";
}

/**
 * How far through `[start, end]` the cursor sits, 0–100.
 *
 * Clamped at both ends rather than trusted. A cursor can legitimately sit
 * outside the window: ahead of it when the operator moved 结束时间 back after
 * sweeping past it, behind it when they moved 开始时间 forward and the sweep
 * has not started yet. Neither is an error, and neither should draw a bar
 * hanging off the end of its track.
 *
 * A window with no width answers 100 — an already-finished sweep — because the
 * only way to get one is an end at or before the start, and the sweep returns
 * immediately in exactly that case.
 */
export function windowPercent(at, start, end) {
  const [cursor, from, to] = [at, start, end].map((value) =>
    typeof value === "number" ? value : Date.parse(value),
  );
  if (!Number.isFinite(cursor) || !Number.isFinite(from) || !Number.isFinite(to)) {
    return 0;
  }
  if (!(to > from)) return 100;
  return Math.min(100, Math.max(0, ((cursor - from) / (to - from)) * 100));
}
