"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";

import {
  jsonRequest,
  type RuleCount,
  type Series,
  type SeriesPoint,
  type SeriesUnit,
} from "@/lib/mjai-api";
import {
  NO_RULE_FILTER,
  RULE_FACETS,
  ruleLabel,
  type RuleFacetKey,
} from "@/lib/rules";
import { cn, dayEnd, dayStart, formatBytes } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type Window = { label: string; unit: SeriesUnit; span: number };

// An hour is the finest bucket the index will group into, and the shortest
// window is a day of them: "did the collector stop this afternoon" is the
// question a day of daily buckets answers with a single bar.
const WINDOWS: Window[] = [
  { label: "24 小时", unit: "hour", span: 24 },
  { label: "48 小时", unit: "hour", span: 48 },
  { label: "7 天", unit: "hour", span: 168 },
  { label: "30 天", unit: "day", span: 30 },
  { label: "90 天", unit: "day", span: 90 },
  { label: "365 天", unit: "day", span: 365 },
];
const DEFAULT_WINDOW = 2;

/** A custom range this many days wide or narrower is bucketed by the hour. */
const HOURLY_RANGE_DAYS = 7;
const MS_PER_DAY = 86_400_000;

/** `YYYY-MM-DD` in the reader's own timezone, which is what a date input wants. */
function calendarDay(at: Date) {
  return `${at.getFullYear()}-${pad2(at.getMonth() + 1)}-${pad2(at.getDate())}`;
}

/**
 * How many days a picked range covers, both ends included — two dates one day
 * apart is two days of data, not one.
 */
function rangeDays(from: string, to: string) {
  const span =
    (new Date(`${to}T00:00:00`).getTime() -
      new Date(`${from}T00:00:00`).getTime()) /
      MS_PER_DAY +
    1;
  return Math.max(1, Math.round(span));
}

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

type SeriesSpec = {
  key: MetricKey;
  label: string;
  /** A `text-*` class; the bars are filled from `currentColor` off the group. */
  tone: string;
};

const BYTE_TICK_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"];

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

/**
 * Hourly buckets are UTC instants rendered in the reader's own timezone: a bar
 * labelled 05:00 to somebody whose clock says 13:00 is worse than no label at
 * all. Daily buckets are a bare `YYYY-MM-DD` and are deliberately *not* parsed
 * — `new Date("2026-07-30")` is UTC midnight, which renders as the 29th
 * anywhere west of Greenwich.
 */
function bucketLabel(point: SeriesPoint, unit: SeriesUnit) {
  if (unit === "day") {
    return point.at;
  }
  const at = new Date(point.at);
  return `${pad2(at.getMonth() + 1)}-${pad2(at.getDate())} ${pad2(at.getHours())}:00`;
}

/** The same instant on an axis, where there is room for one field, not three. */
function bucketTick(point: SeriesPoint, unit: SeriesUnit, span: number) {
  if (unit === "day") {
    return point.at.slice(5);
  }
  const at = new Date(point.at);
  return span <= 48
    ? `${pad2(at.getHours())}:00`
    : `${pad2(at.getMonth() + 1)}-${pad2(at.getDate())}`;
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
  // Never finer than one unit per division: a bucket holding a single record
  // would otherwise be measured out in quarters of a record.
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
 * Bars, not a line. These are counts per bucket, and a line drawn through them
 * joins one bucket to the next with a segment that claims values for the time
 * between — on a corpus that sat at zero for three weeks and then took 641,475
 * records in one afternoon, that segment is the most prominent thing on the
 * chart and it describes nothing that happened. A missing bar is a bucket with
 * nothing in it.
 *
 * Still no charting library: this is a rectangle per bucket and an axis.
 */
function TrendChart({
  points,
  unit,
  series,
  format,
  tick,
  binary,
  title,
  description,
}: {
  points: SeriesPoint[];
  unit: SeriesUnit;
  series: SeriesSpec[];
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
  // 365 buckets for 24 under a chart that keeps its state, so an index that was
  // valid when it was set can outlive the array it indexes.
  const index = hovered === null ? null : Math.min(hovered, points.length - 1);
  const active = index === null ? null : points[index];

  // At most five, always both ends: a 168-bucket axis labelled at every bucket
  // is a grey band, and one labelled only in the middle never says what it
  // covers.
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

  // Anchored by which fifth of the chart the pointer is in, so a readout for
  // the first or last bucket does not hang off the side of the card.
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
              aria-label={`${title}：${points.length} 个${unit === "hour" ? "小时" : "天"}，峰值 ${format(peak)}`}
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
                  {points.map((point, bucketIndex) => {
                    const value = point[key];
                    if (value <= 0) {
                      return null;
                    }
                    const top = y(value);
                    return (
                      <rect
                        key={point.at}
                        x={
                          PAD.left +
                          slot * bucketIndex +
                          slot * 0.15 +
                          barW * seriesIndex
                        }
                        y={top}
                        width={barW}
                        // A bucket that rounds to nothing still gets a mark: a
                        // bar of zero height and a bucket with no records look
                        // the same and are not.
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
                // the middle of their own bucket: half a date centred on the
                // last slot hangs past the right margin, and on a phone-width
                // card that clips the most recent bucket off the axis.
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
                    {points[tickIndex]
                      ? bucketTick(points[tickIndex], unit, points.length)
                      : ""}
                  </text>
                );
              })}
            </svg>
          )}
          {/* Announced as well as drawn: this is the only textual form the
              per-bucket numbers take, and the arrow keys move it. */}
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
              {active ? bucketLabel(active, unit) : ""}
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

/** Room first, so the panel is scannable by colour before it is read. */
function ruleTone(rule: string) {
  if (rule.includes("throne")) {
    return "bg-chart-1";
  }
  if (rule.includes("jade")) {
    return "bg-chart-2";
  }
  if (rule.includes("gold")) {
    return "bg-chart-3";
  }
  return "bg-chart-4";
}

/**
 * Which modes the window's games were played in. Bar length is relative to the
 * busiest mode rather than to the total: at a realistic spread the leader holds
 * a third of the window, and scaling to the total leaves every bar a stub.
 */
function RuleBreakdown({ rules }: { rules: RuleCount[] }) {
  const total = rules.reduce((sum, rule) => sum + rule.games, 0);
  const busiest = rules[0]?.games ?? 0;
  return (
    <div className="rounded-xl border bg-card text-card-foreground">
      <div className="flex flex-col gap-1 px-5 pt-4 sm:flex-row sm:items-baseline sm:justify-between">
        <div>
          <h3 className="text-sm leading-none font-medium">场次分布</h3>
          <p className="text-muted-foreground mt-1.5 text-xs">
            窗口内开打的对局按场次分组，与上面的「对局」是同一批记录。
          </p>
        </div>
        <span className="text-muted-foreground shrink-0 font-mono text-xs tabular-nums">
          共 {count(total)} 局 · {rules.length} 个场次
        </span>
      </div>
      <div className="px-5 pt-3 pb-4">
        {rules.length === 0 ? (
          <p className="text-muted-foreground py-6 text-center text-sm">
            这段时间没有对局
          </p>
        ) : (
          <ul className="grid gap-x-8 gap-y-2.5 sm:grid-cols-2 xl:grid-cols-3">
            {rules.map((rule) => (
              <li key={rule.rule || "unknown"} className="space-y-1">
                <div className="flex items-baseline justify-between gap-3 text-xs">
                  <span className="truncate">{ruleLabel(rule.rule)}</span>
                  <span className="text-muted-foreground shrink-0 font-mono tabular-nums">
                    {count(rule.games)}
                    <span className="ml-1.5">
                      {total > 0
                        ? `${((rule.games / total) * 100).toFixed(1)}%`
                        : "—"}
                    </span>
                  </span>
                </div>
                <div className="bg-muted h-1.5 overflow-hidden rounded-full">
                  <div
                    className={cn("h-full rounded-full", ruleTone(rule.rule))}
                    style={{
                      width: `${busiest > 0 ? (rule.games / busiest) * 100 : 0}%`,
                    }}
                  />
                </div>
              </li>
            ))}
          </ul>
        )}
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
    <div className="bg-card rounded-xl border px-4 py-3">
      <p className="text-muted-foreground text-xs">{label}</p>
      <p className="mt-1.5 font-mono text-xl font-semibold tabular-nums">
        {value}
      </p>
      <p className="text-muted-foreground mt-1 text-[11px]">{detail}</p>
    </div>
  );
}

export function StatsTrends() {
  const [choice, setChoice] = useState<number | "custom">(DEFAULT_WINDOW);
  const [range, setRange] = useState({ from: "", to: "" });
  const [modes, setModes] =
    useState<Record<RuleFacetKey, string[]>>(NO_RULE_FILTER);
  const [series, setSeries] = useState<Series | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const filtered = RULE_FACETS.some((facet) => modes[facet.key].length > 0);
  const custom = choice === "custom";
  const picked = custom && range.from !== "" && range.to !== "";
  // The granularity a custom range implies, rather than another control to
  // set: a week or less is worth an hour a bar, and past that the hourly
  // ceiling would silently clip the range the reader just picked.
  const customUnit: SeriesUnit =
    picked && rangeDays(range.from, range.to) <= HOURLY_RANGE_DAYS
      ? "hour"
      : "day";
  const unit = custom ? customUnit : WINDOWS[choice as number].unit;

  // One string identifies the request, so it is also the only thing the poll
  // has to watch. Depending on the three arrays directly would refetch on every
  // render, because a fresh array is never equal to the last one.
  const search = useMemo(() => {
    if (custom && !picked) {
      // Nothing to ask for yet. Returning null rather than a half range keeps
      // the last answer on screen instead of replacing it with an error the
      // reader caused by opening the picker.
      return null;
    }
    const params = new URLSearchParams({ unit });
    if (custom) {
      // The bounds are the reader's own calendar days, not UTC's — the same
      // day the timestamps beside them are rendered in.
      params.set("from", dayStart(range.from));
      params.set("to", dayEnd(range.to));
    } else {
      params.set("span", String(WINDOWS[choice as number].span));
    }
    for (const facet of RULE_FACETS) {
      if (modes[facet.key].length > 0) {
        params.set(facet.key, modes[facet.key].join(","));
      }
    }
    return params.toString();
  }, [choice, custom, picked, unit, range, modes]);

  const toggle = (key: RuleFacetKey, value: string) =>
    setModes((previous) => ({
      ...previous,
      [key]: previous[key].includes(value)
        ? previous[key].filter((kept) => kept !== value)
        : [...previous[key], value],
    }));

  // Everything the poll needs lives inside the effect, one instance per window.
  // The overview's shared in-flight ref would be wrong here: it exists to stop
  // a slow poll from stacking, and it cannot tell that apart from the click
  // that changed the window — so pressing "24 小时" while the 7-day poll was in
  // flight was dropped, leaving the button lit over a week of data until the
  // next tick. A flag scoped to the effect blocks only its own repeats, and
  // `live` discards the answer to a window nobody is looking at any more.
  useEffect(() => {
    if (search === null) {
      return;
    }
    let live = true;
    let busy = false;
    const load = async () => {
      if (busy) {
        return;
      }
      busy = true;
      try {
        const next = await jsonRequest<Series>(`/api/stats/series?${search}`);
        if (live) {
          setSeries(next);
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
    // A minute either way: on an hourly window that is a sixtieth of a bucket,
    // and on a daily one the finest thing on the page is a day.
    const timer = globalThis.setInterval(() => void load(), 60_000);
    return () => {
      live = false;
      globalThis.clearInterval(timer);
    };
  }, [search]);

  const summary = useMemo(() => {
    const points = series?.points;
    if (!points?.length) {
      return null;
    }
    const sum = (key: MetricKey) =>
      points.reduce((total, point) => total + point[key], 0);
    const records = sum("records");
    const games = sum("games");
    const raw = sum("raw_bytes");
    const packed = sum("compressed_bytes");
    const collecting = points.filter((point) => point.records > 0).length;
    const busiest = points.reduce((best, point) =>
      point.records > best.records ? point : best,
    );
    const top = series?.rules[0];
    return {
      records,
      games,
      raw,
      packed,
      collecting,
      busiest,
      top,
      // Guarded rather than shown as 0%: nothing stored and perfect compression
      // are not the same reading.
      ratio: raw > 0 ? `${((1 - packed / raw) * 100).toFixed(1)}%` : "—",
      average: collecting > 0 ? Math.round(records / collecting) : 0,
    };
  }, [series]);

  const bucketName = (series?.unit ?? unit) === "hour" ? "小时" : "天";

  return (
    <div className="space-y-4">
      <div className="bg-muted inline-flex flex-wrap rounded-lg p-[3px]">
        {WINDOWS.map((option, position) => (
          <Button
            key={option.label}
            size="sm"
            variant="ghost"
            className={cn(
              "h-7 px-3 text-xs",
              position === choice &&
                "bg-background text-foreground hover:bg-background shadow-sm",
            )}
            onClick={() => setChoice(position)}
          >
            {option.label}
          </Button>
        ))}
        <Button
          size="sm"
          variant="ghost"
          className={cn(
            "h-7 px-3 text-xs",
            custom
              ? "bg-background text-foreground hover:bg-background shadow-sm"
              : "text-muted-foreground",
          )}
          onClick={() => {
            // Opened onto the last week rather than onto two empty inputs, so
            // the picker answers with something before it is touched.
            if (!custom) {
              const today = new Date();
              const start = new Date(today);
              start.setDate(start.getDate() - (HOURLY_RANGE_DAYS - 1));
              setRange({ from: calendarDay(start), to: calendarDay(today) });
            }
            setChoice("custom");
          }}
        >
          自定义
        </Button>
      </div>

      {custom ? (
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <Input
            type="date"
            aria-label="起始日期"
            className="h-7 w-auto text-xs"
            value={range.from}
            max={range.to || undefined}
            onChange={(event) =>
              setRange((previous) => ({
                ...previous,
                from: event.target.value,
              }))
            }
          />
          <span className="text-muted-foreground">到</span>
          <Input
            type="date"
            aria-label="结束日期"
            className="h-7 w-auto text-xs"
            value={range.to}
            min={range.from || undefined}
            onChange={(event) =>
              setRange((previous) => ({ ...previous, to: event.target.value }))
            }
          />
          <span className="text-muted-foreground">
            {!picked
              ? "选好起止两天才会查询"
              : customUnit === "hour"
                ? `含两端共 ${rangeDays(range.from, range.to)} 天，按小时分桶`
                : `含两端共 ${count(rangeDays(range.from, range.to))} 天，按天分桶${
                    rangeDays(range.from, range.to) > 365
                      ? "；超过 365 天，只画最近的 365 天"
                      : ""
                  }`}
          </span>
        </div>
      ) : null}

      {/* Multi-select, and an untouched facet means all of it — so "四麻" alone
          is every 四麻 room at every length, not one mode. Filtering on any
          facet does drop the records whose converter left no mode on them:
          they are not 三麻 and not 四麻, and a chart that keeps them under
          either heading is lying about which population it drew. */}
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
        {RULE_FACETS.map((facet) => (
          <div key={facet.key} className="flex items-center gap-2">
            <span className="text-muted-foreground text-xs">{facet.label}</span>
            {/* The same segmented shape as the window picker above it. An
                unselected option drawn as bare text is indistinguishable from
                the label beside it, and nothing on the row then looks
                clickable — which is the whole point of the row. */}
            <div className="bg-muted inline-flex rounded-lg p-[3px]">
              {facet.options.map((option) => {
                const on = modes[facet.key].includes(option.value);
                return (
                  <Button
                    key={option.value}
                    size="sm"
                    variant="ghost"
                    aria-pressed={on}
                    className={cn(
                      "h-7 px-2.5 text-xs",
                      on
                        ? "bg-background text-foreground hover:bg-background shadow-sm"
                        : "text-muted-foreground",
                    )}
                    onClick={() => toggle(facet.key, option.value)}
                  >
                    {option.label}
                  </Button>
                );
              })}
            </div>
          </div>
        ))}
        {filtered ? (
          <Button
            size="sm"
            variant="ghost"
            className="text-muted-foreground h-7 px-2.5 text-xs"
            onClick={() => setModes(NO_RULE_FILTER)}
          >
            清除筛选
          </Button>
        ) : null}
      </div>

      {error ? (
        <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
          无法读取趋势数据：{error}
        </p>
      ) : null}

      {summary && series ? (
        <>
          <section className="grid grid-cols-2 gap-3 xl:grid-cols-4">
            <Tile
              label="窗口内入库"
              value={count(summary.records)}
              detail={`${summary.collecting} 个${bucketName}有数据 · 均 ${count(summary.average)}`}
            />
            <Tile
              label="窗口内对局"
              value={count(summary.games)}
              detail={
                summary.top && summary.games > 0
                  ? `${ruleLabel(summary.top.rule)} 占 ${((summary.top.games / summary.games) * 100).toFixed(0)}%`
                  : "窗口内没有对局"
              }
            />
            <Tile
              label={`最忙的一${bucketName}`}
              value={
                summary.busiest.records > 0
                  ? bucketLabel(summary.busiest, series.unit)
                  : "—"
              }
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
              points={series.points}
              unit={series.unit}
              title={`每${bucketName}入库与对局`}
              description={`蓝柱是记录进索引的那${bucketName}，青柱是这局牌开打的那${bucketName}——一次历史导入只抬高前者。`}
              format={count}
              tick={tickCount}
              series={[
                { key: "records", label: "入库", tone: "text-chart-1" },
                { key: "games", label: "对局", tone: "text-chart-2" },
              ]}
            />
            <TrendChart
              points={series.points}
              unit={series.unit}
              title={`每${bucketName}新增数据量`}
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
          <RuleBreakdown rules={series.rules} />
        </>
      ) : (
        <div className="bg-card text-muted-foreground rounded-xl border px-4 py-16 text-center text-sm">
          {loading ? "正在读取趋势数据…" : "趋势数据不可用"}
        </div>
      )}
    </div>
  );
}
