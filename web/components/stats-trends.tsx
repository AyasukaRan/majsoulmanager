"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";

import { jsonRequest, type DailyPoint, type DailyStats } from "@/lib/mjai-api";
import { cn, formatBytes } from "@/lib/utils";
import { Button } from "@/components/ui/button";

const WINDOWS = [7, 30, 90, 365];

// The chart is drawn at device pixels rather than in a scaled viewBox. A
// viewBox stretched to the card's width scales everything in it — the first
// version drew 10px labels that arrived on screen at 17px and a 1.75px bar
// edge that arrived at 3px, which is most of what made it look coarse. The
// width is measured instead, and every number below is the number of pixels it
// ends up being.
const HEIGHT = 196;
const PAD = { top: 12, right: 8, bottom: 22, left: 48 };
const DIVISIONS = 4;

type MetricKey = "records" | "games" | "raw_bytes" | "compressed_bytes";

type Series = {
  key: MetricKey;
  label: string;
  /** A `text-*` class; the bars are filled from `currentColor` off the group. */
  tone: string;
};

const BYTE_TICK_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"];

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

/** Axis ticks are compact where the readout is exact: `80万`, not `800,000`. */
function tickCount(value: number) {
  return value === 0
    ? "0"
    : new Intl.NumberFormat("zh-CN", {
        notation: "compact",
        maximumFractionDigits: 1,
      }).format(value);
}

/** And `8 GiB`, not `8.00 GiB` — a gridline label carries no padding zeros. */
function tickBytes(bytes: number) {
  let unit = 0;
  let value = bytes;
  while (value >= 1024 && unit < BYTE_TICK_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${Math.round(value * 100) / 100} ${BYTE_TICK_UNITS[unit]}`;
}

/**
 * The value of the top gridline, chosen so the four labels under it are round
 * numbers. Rounding the *step* and multiplying is what keeps them round;
 * scaling the peak instead gives an axis whose quarters are 3,271 and 6,542.
 *
 * `binary` rounds the step inside the unit the byte labels will use, because a
 * step that is a round number of bytes is not a round number of GiB: the first
 * version labelled a storage axis `7.45 GiB / 14.90 GiB / 22.35 GiB`.
 */
function niceMax(peak: number, binary = false) {
  if (peak <= 0) {
    return DIVISIONS;
  }
  const rough = peak / DIVISIONS;
  const unit = binary
    ? 1024 **
      Math.min(4, Math.max(0, Math.floor(Math.log(rough) / Math.log(1024))))
    : 1;
  const scaled = rough / unit;
  const magnitude = 10 ** Math.floor(Math.log10(scaled));
  const factor =
    [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].find(
      (candidate) => scaled <= candidate * magnitude,
    ) ?? 10;
  const step = factor * magnitude * unit;
  // Never finer than one unit per division: a day holding a single record would
  // otherwise be measured out in quarters of a record.
  return Math.max(
    DIVISIONS,
    binary ? step * DIVISIONS : Math.ceil(step) * DIVISIONS,
  );
}

/** The rendered width of an element, or 0 before it has been measured. */
function useMeasuredWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const node = ref.current;
    if (!node) {
      return;
    }
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.round(entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}

/**
 * Bars, not a line. These are daily counts, and a line drawn through them joins
 * one day to the next with a segment that claims values for the hours between —
 * on a corpus that sat at zero for three weeks and then took 641,475 records in
 * one afternoon, that segment is the most prominent thing on the chart and it
 * describes nothing that happened. A missing bar is a day with nothing in it.
 *
 * Still no charting library: this is a rectangle per day and an axis.
 */
function TrendChart({
  points,
  series,
  format,
  tick,
  binary,
  title,
  description,
}: {
  points: DailyPoint[];
  series: Series[];
  format: (value: number) => string;
  tick: (value: number) => string;
  binary?: boolean;
  title: string;
  description: string;
}) {
  const [hovered, setHovered] = useState<number | null>(null);
  const [ref, width] = useMeasuredWidth();

  const peak = useMemo(
    () =>
      Math.max(
        0,
        ...points.flatMap((point) => series.map(({ key }) => point[key])),
      ),
    [points, series],
  );
  const max = useMemo(() => niceMax(peak, binary), [peak, binary]);

  const plotW = Math.max(1, width - PAD.left - PAD.right);
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const base = PAD.top + plotH;
  const slot = plotW / Math.max(1, points.length);
  const barW = Math.max(0.6, (slot * 0.7) / series.length);
  const y = (value: number) => PAD.top + plotH * (1 - value / max);

  // Clamped at render, not where the pointer set it: the window buttons swap
  // 365 days for 7 under a chart that keeps its state, so an index that was
  // valid when it was set can outlive the array it indexes.
  const index = hovered === null ? null : Math.min(hovered, points.length - 1);
  const active = index === null ? null : points[index];

  // At most five, always both ends: a 365-day axis labelled at every day is a
  // grey band, and one labelled only in the middle never says what it covers.
  const ticks = useMemo(() => {
    const wanted = Math.min(5, points.length);
    return [
      ...new Set(
        Array.from({ length: wanted }, (_, step) =>
          Math.round((step * (points.length - 1)) / Math.max(1, wanted - 1)),
        ),
      ),
    ];
  }, [points.length]);

  const locate = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      const box = event.currentTarget.getBoundingClientRect();
      if (box.width === 0 || points.length === 0) {
        return;
      }
      const x = ((event.clientX - box.left) / box.width) * width;
      const slotIndex = Math.floor((x - PAD.left) / (plotW / points.length));
      setHovered(Math.min(points.length - 1, Math.max(0, slotIndex)));
    },
    [points.length, width, plotW],
  );

  // Anchored by which third of the chart the pointer is in, so a readout for
  // the first or last day does not hang off the side of the card.
  const anchorRatio = index === null ? 0.5 : (index + 0.5) / points.length;
  const anchor =
    anchorRatio < 0.2
      ? "translateX(0)"
      : anchorRatio > 0.8
        ? "translateX(-100%)"
        : "translateX(-50%)";

  return (
    <div className="rounded-xl border bg-card text-card-foreground">
      <div className="flex flex-col gap-2 px-5 pt-4 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
        <div>
          <h3 className="text-sm leading-none font-medium">{title}</h3>
          <p className="text-muted-foreground mt-1.5 text-xs leading-relaxed">
            {description}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-3 text-xs">
          {series.map(({ key, label, tone }) => (
            <span key={key} className="flex items-center gap-1.5">
              <span
                aria-hidden="true"
                className={cn("size-2 rounded-[3px] bg-current", tone)}
              />
              <span className="text-muted-foreground">{label}</span>
            </span>
          ))}
        </div>
      </div>
      {/* The measured box and the tooltip's containing block are the same
          element, and it is the plot rather than the padded card: a readout
          positioned against the card would have to carry the padding as a
          constant, and be silently wrong the day the padding changes. */}
      <div className="px-5 pt-3 pb-3">
        <div ref={ref} className="relative">
          {width === 0 ? (
            <div style={{ height: HEIGHT }} />
          ) : (
            <svg
              width={width}
              height={HEIGHT}
              viewBox={`0 0 ${width} ${HEIGHT}`}
              // `pan-y`, not `none`: these fill most of a phone screen, and
              // `touch-action: none` would mean a finger landing on one scrolls
              // nothing at all. A horizontal drag is still ours under `pan-y`.
              className="w-full touch-pan-y select-none"
              role="img"
              tabIndex={0}
              aria-label={`${title}：${points.length} 天，峰值 ${format(peak)}`}
              onPointerMove={locate}
              onPointerDown={locate}
              onPointerLeave={() => setHovered(null)}
              onBlur={() => setHovered(null)}
              onKeyDown={(event) => {
                const step =
                  event.key === "ArrowLeft"
                    ? -1
                    : event.key === "ArrowRight"
                      ? 1
                      : 0;
                if (step === 0) {
                  return;
                }
                event.preventDefault();
                setHovered((previous) =>
                  Math.min(
                    points.length - 1,
                    Math.max(0, (previous ?? points.length - 1) + step),
                  ),
                );
              }}
            >
              {index === null ? null : (
                <rect
                  x={PAD.left + slot * index}
                  y={PAD.top}
                  width={slot}
                  height={plotH}
                  className="fill-foreground"
                  opacity={0.06}
                />
              )}
              {Array.from({ length: DIVISIONS + 1 }, (_, step) => {
                const value = (max * step) / DIVISIONS;
                const gridY = y(value);
                return (
                  <g key={step}>
                    <line
                      x1={PAD.left}
                      x2={width - PAD.right}
                      y1={gridY}
                      y2={gridY}
                      className="stroke-border"
                      strokeWidth={1}
                      strokeDasharray={step === 0 ? undefined : "2 4"}
                    />
                    <text
                      x={PAD.left - 10}
                      y={gridY + 3}
                      textAnchor="end"
                      className="fill-muted-foreground text-[10px] tabular-nums"
                    >
                      {tick(value)}
                    </text>
                  </g>
                );
              })}
              {series.map(({ key, tone }, seriesIndex) => (
                <g key={key} className={tone}>
                  {points.map((point, dayIndex) => {
                    const value = point[key];
                    if (value <= 0) {
                      return null;
                    }
                    const top = y(value);
                    return (
                      <rect
                        key={point.day}
                        x={
                          PAD.left +
                          slot * dayIndex +
                          slot * 0.15 +
                          barW * seriesIndex
                        }
                        y={top}
                        width={barW}
                        // A day that rounds to nothing still gets a mark: a bar
                        // of zero height and a day with no records look the same
                        // and are not.
                        height={Math.max(1, base - top)}
                        rx={Math.min(1.5, barW / 2)}
                        fill="currentColor"
                      />
                    );
                  })}
                </g>
              ))}
              {ticks.map((tickIndex) => {
                // The end labels anchor to the edges of the plot rather than to
                // the middle of their own day: half a date centred on the last
                // slot hangs past the right margin, and on a phone-width card
                // that clips the most recent day off the axis.
                const first = tickIndex === 0;
                const last = tickIndex === points.length - 1;
                return (
                  <text
                    key={tickIndex}
                    x={
                      first
                        ? PAD.left
                        : last
                          ? width - PAD.right
                          : PAD.left + slot * tickIndex + slot / 2
                    }
                    y={HEIGHT - 6}
                    textAnchor={first ? "start" : last ? "end" : "middle"}
                    className="fill-muted-foreground text-[10px] tabular-nums"
                  >
                    {points[tickIndex]?.day.slice(5)}
                  </text>
                );
              })}
            </svg>
          )}
          {/* Announced as well as drawn: this is the only textual form the
              per-day numbers take, and the arrow keys move it. */}
          <div
            aria-live="polite"
            className={cn(
              "bg-popover pointer-events-none absolute top-1 z-10 rounded-lg border px-2.5 py-1.5 text-xs shadow-md",
              active ? "block" : "hidden",
            )}
            style={{
              left: PAD.left + slot * ((index ?? 0) + 0.5),
              transform: anchor,
            }}
          >
            <div className="text-muted-foreground font-mono tabular-nums">
              {active?.day}
            </div>
            {series.map(({ key, label, tone }) => (
              <div
                key={key}
                className="mt-1 flex items-center gap-2 whitespace-nowrap"
              >
                <span
                  aria-hidden="true"
                  className={cn("size-2 rounded-[3px] bg-current", tone)}
                />
                <span className="text-muted-foreground">{label}</span>
                <span className="ml-auto font-mono font-medium tabular-nums">
                  {active ? format(active[key]) : "—"}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function Tile({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-xl border bg-card px-4 py-3">
      <p className="text-muted-foreground text-xs">{label}</p>
      <p className="mt-1.5 font-mono text-xl font-semibold tabular-nums">
        {value}
      </p>
      <p className="text-muted-foreground mt-1 text-[11px]">{detail}</p>
    </div>
  );
}

export function StatsTrends() {
  const [days, setDays] = useState(30);
  const [points, setPoints] = useState<DailyPoint[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Everything the poll needs lives inside the effect, one instance per window.
  // The overview's shared in-flight ref would be wrong here: it exists to stop
  // a slow poll from stacking, and it cannot tell that apart from the click
  // that changed the window — so pressing "7 天" while the 30-day poll was in
  // flight was dropped, leaving the button lit over a month of data until the
  // next tick. A flag scoped to the effect blocks only its own repeats, and
  // `live` discards the answer to a window nobody is looking at any more.
  useEffect(() => {
    let live = true;
    let busy = false;
    const load = async () => {
      if (busy) {
        return;
      }
      busy = true;
      try {
        const stats = await jsonRequest<DailyStats>(
          `/api/stats/daily?days=${days}`,
        );
        if (live) {
          setPoints(stats.days);
          setError(null);
        }
      } catch (caught) {
        if (live) {
          setError(caught instanceof Error ? caught.message : "读取趋势失败");
        }
      } finally {
        busy = false;
        if (live) {
          setLoading(false);
        }
      }
    };
    void load();
    // A minute, not the overview's fifteen seconds: the finest thing on this
    // page is a day.
    const timer = window.setInterval(() => void load(), 60_000);
    return () => {
      live = false;
      window.clearInterval(timer);
    };
  }, [days]);

  const summary = useMemo(() => {
    if (!points?.length) {
      return null;
    }
    const sum = (key: MetricKey) =>
      points.reduce((total, point) => total + point[key], 0);
    const records = sum("records");
    const raw = sum("raw_bytes");
    const packed = sum("compressed_bytes");
    const collecting = points.filter((point) => point.records > 0).length;
    const busiest = points.reduce((best, point) =>
      point.records > best.records ? point : best,
    );
    const firstPlayed = points.find((point) => point.games > 0);
    return {
      records,
      games: sum("games"),
      raw,
      packed,
      collecting,
      busiest,
      firstPlayed,
      // Guarded rather than shown as 0%: nothing stored and perfect compression
      // are not the same reading.
      ratio: raw > 0 ? `${((1 - packed / raw) * 100).toFixed(1)}%` : "—",
      daily: collecting > 0 ? Math.round(records / collecting) : 0,
    };
  }, [points]);

  return (
    <div className="space-y-4">
      <div className="bg-muted inline-flex rounded-lg p-[3px]">
        {WINDOWS.map((window) => (
          <Button
            key={window}
            size="sm"
            variant="ghost"
            className={cn(
              "h-7 px-3 text-xs",
              window === days &&
                "bg-background text-foreground shadow-sm hover:bg-background",
            )}
            onClick={() => setDays(window)}
          >
            {window} 天
          </Button>
        ))}
      </div>

      {error ? (
        <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
          无法读取趋势数据：{error}
        </p>
      ) : null}

      {summary && points ? (
        <>
          <section className="grid grid-cols-2 gap-3 xl:grid-cols-4">
            <Tile
              label="窗口内入库"
              value={count(summary.records)}
              detail={`${summary.collecting} 天有数据 · 日均 ${count(summary.daily)}`}
            />
            <Tile
              label="窗口内对局"
              value={count(summary.games)}
              detail={
                summary.firstPlayed
                  ? `最早开打于 ${summary.firstPlayed.day}`
                  : "窗口内没有对局"
              }
            />
            <Tile
              label="最忙的一天"
              value={summary.busiest.records > 0 ? summary.busiest.day : "—"}
              detail={`入库 ${count(summary.busiest.records)} 条`}
            />
            <Tile
              label="压缩率"
              value={summary.ratio}
              detail={`${formatBytes(summary.raw)} → ${formatBytes(summary.packed)}`}
            />
          </section>
          <section className="grid gap-4 xl:grid-cols-2">
            <TrendChart
              points={points}
              title="每日入库与对局"
              description="蓝柱是记录进索引的那天，青柱是这局牌开打的那天——一次历史导入只抬高前者。"
              format={count}
              tick={tickCount}
              series={[
                { key: "records", label: "入库", tone: "text-chart-1" },
                { key: "games", label: "对局", tone: "text-chart-2" },
              ]}
            />
            <TrendChart
              points={points}
              title="每日新增数据量"
              description="按入库时间统计的牌谱体积，两根柱子的高度差就是 zstd 省下的部分。"
              format={formatBytes}
              tick={tickBytes}
              binary
              series={[
                { key: "raw_bytes", label: "原始", tone: "text-chart-1" },
                {
                  key: "compressed_bytes",
                  label: "压缩后",
                  tone: "text-chart-2",
                },
              ]}
            />
          </section>
        </>
      ) : (
        <div className="rounded-xl border bg-card px-4 py-16 text-center text-sm text-muted-foreground">
          {loading ? "正在读取趋势数据…" : "趋势数据不可用"}
        </div>
      )}
    </div>
  );
}
