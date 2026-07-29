"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";

import { jsonRequest, type DailyPoint, type DailyStats } from "@/lib/mjai-api";
import { cn, formatBytes } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

const WINDOWS = [7, 30, 90, 365];

// viewBox units. The chart is laid out once at this size and scaled to the card
// by CSS, so nothing below has to measure how wide it ended up — the only place
// the real width is needed is the pointer, which converts back through the
// element's bounding box.
const WIDTH = 760;
const HEIGHT = 220;
const PAD = { top: 12, right: 14, bottom: 24, left: 68 };
const PLOT_W = WIDTH - PAD.left - PAD.right;
const PLOT_H = HEIGHT - PAD.top - PAD.bottom;
const DIVISIONS = 4;

type MetricKey = "records" | "games" | "raw_bytes" | "compressed_bytes";

type Series = {
  key: MetricKey;
  label: string;
  /**
   * A `text-*` class; the shapes draw in `currentColor` off the group. Both
   * charts use `chart-1` and `chart-2` and no others, because the dark theme
   * renders the five chart tokens as a greyscale ramp and `chart-3` downwards
   * sit within 0.17 lightness of the card they are drawn on.
   */
  tone: string;
};

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

/**
 * The value of the top gridline, chosen so the four labels under it are round
 * numbers. Picking the *step* and multiplying is what keeps them round: scaling
 * the peak instead gives an axis whose quarters are 3,271 and 6,542.
 */
function niceMax(peak: number) {
  if (peak <= 0) {
    return DIVISIONS;
  }
  const rough = peak / DIVISIONS;
  const magnitude = 10 ** Math.floor(Math.log10(rough));
  const factor =
    [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].find(
      (candidate) => rough <= candidate * magnitude,
    ) ?? 10;
  // Never finer than one unit per division: a day holding a single record would
  // otherwise be measured out in quarters of a record.
  return Math.max(DIVISIONS, Math.ceil(factor * magnitude) * DIVISIONS);
}

function pointX(index: number, total: number) {
  return total > 1
    ? PAD.left + (index * PLOT_W) / (total - 1)
    : PAD.left + PLOT_W / 2;
}

function pointY(value: number, max: number) {
  return PAD.top + PLOT_H * (1 - value / max);
}

function linePath(values: number[], max: number) {
  return values
    .map(
      (value, index) =>
        `${index === 0 ? "M" : "L"}${pointX(index, values.length).toFixed(2)} ${pointY(value, max).toFixed(2)}`,
    )
    .join(" ");
}

function areaPath(values: number[], max: number) {
  const floor = PAD.top + PLOT_H;
  return `${linePath(values, max)} L${pointX(values.length - 1, values.length).toFixed(2)} ${floor} L${pointX(0, values.length).toFixed(2)} ${floor} Z`;
}

/**
 * A line chart in plain SVG. Deliberately not a charting library: four series
 * over at most a year of daily points is a path builder and an axis, and the
 * smallest library that draws it is larger than everything else the console
 * ships put together.
 */
function TrendChart({
  points,
  series,
  format,
  title,
  description,
}: {
  points: DailyPoint[];
  series: Series[];
  format: (value: number) => string;
  title: string;
  description: string;
}) {
  const [hovered, setHovered] = useState<number | null>(null);

  // Kept apart from `max`, which is the axis ceiling above it: the two differ by
  // up to a third, and the label a screen reader reads out has to be the number
  // in the data rather than the one the gridlines were rounded to.
  const peak = useMemo(
    () =>
      Math.max(
        0,
        ...points.flatMap((point) => series.map(({ key }) => point[key])),
      ),
    [points, series],
  );
  const max = useMemo(() => niceMax(peak), [peak]);

  // Never more than six, and always the ends: a 365-point window labelled at
  // every point is a grey band, and one labelled only in the middle does not
  // say what range it covers.
  const ticks = useMemo(() => {
    const wanted = Math.min(6, points.length);
    return [
      ...new Set(
        Array.from({ length: wanted }, (_, index) =>
          Math.round((index * (points.length - 1)) / Math.max(1, wanted - 1)),
        ),
      ),
    ];
  }, [points.length]);

  // Clamped at render, not at the pointer: the window buttons swap 365 points
  // for 7 under a component that keeps its state, so an index that was valid
  // when the pointer set it can outlive the array it indexed.
  const index =
    hovered === null ? null : Math.min(hovered, points.length - 1);
  // Falls back to the last day so the readout is populated before the pointer
  // ever reaches the chart, and on touch, where there is no hover at all.
  const active = points[index ?? points.length - 1];

  const locate = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      const box = event.currentTarget.getBoundingClientRect();
      if (box.width === 0 || points.length === 0) {
        return;
      }
      // Back out of rendered pixels into viewBox units, which is the one step
      // that lets the rest of the file ignore the element's real size.
      const units = ((event.clientX - box.left) / box.width) * WIDTH;
      const ratio = (units - PAD.left) / PLOT_W;
      const index = Math.round(ratio * (points.length - 1));
      setHovered(Math.min(points.length - 1, Math.max(0, index)));
    },
    [points.length],
  );

  return (
    <Card className="shadow-none">
      <CardHeader className="gap-1">
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
        {/* Announced, not just drawn: this readout is the only textual form the
            per-day numbers take, and arrow keys move it without moving a
            pointer. */}
        <div
          aria-live="polite"
          className="flex flex-wrap items-center gap-x-4 gap-y-1 pt-1 text-xs"
        >
          <span className="font-mono text-muted-foreground">
            {active?.day ?? "—"}
          </span>
          {series.map(({ key, label, tone }) => (
            <span key={key} className="flex items-center gap-1.5">
              <span
                aria-hidden="true"
                className={cn("size-2 rounded-full bg-current", tone)}
              />
              <span className="text-muted-foreground">{label}</span>
              <span className="font-mono font-medium">
                {active ? format(active[key]) : "—"}
              </span>
            </span>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          // `pan-y`, not `none`: the charts fill most of a phone screen, and
          // `touch-action: none` would mean a finger landing on one scrolls
          // nothing at all. A horizontal drag is still not a browser gesture
          // under `pan-y`, so scrubbing survives.
          className="h-auto w-full touch-pan-y select-none"
          role="img"
          tabIndex={0}
          aria-label={`${title}：${points.length} 天，峰值 ${format(peak)}`}
          onPointerMove={locate}
          onPointerDown={locate}
          onPointerLeave={() => setHovered(null)}
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
          {Array.from({ length: DIVISIONS + 1 }, (_, step) => {
            const value = (max * step) / DIVISIONS;
            const y = pointY(value, max);
            return (
              <g key={step}>
                <line
                  x1={PAD.left}
                  x2={WIDTH - PAD.right}
                  y1={y}
                  y2={y}
                  className="stroke-border"
                  strokeWidth={1}
                />
                <text
                  x={PAD.left - 8}
                  y={y + 3.5}
                  textAnchor="end"
                  className="fill-muted-foreground font-mono text-[10px]"
                >
                  {format(value)}
                </text>
              </g>
            );
          })}

          {series.map(({ key, tone }) => {
            const values = points.map((point) => point[key]);
            return (
              <g key={key} className={tone}>
                <path d={areaPath(values, max)} fill="currentColor" opacity={0.1} />
                <path
                  d={linePath(values, max)}
                  fill="none"
                  stroke="currentColor"
                  strokeWidth={1.75}
                  strokeLinejoin="round"
                  strokeLinecap="round"
                />
              </g>
            );
          })}

          {ticks.map((index) => (
            <text
              key={index}
              x={pointX(index, points.length)}
              y={HEIGHT - 6}
              textAnchor={
                index === 0
                  ? "start"
                  : index === points.length - 1
                    ? "end"
                    : "middle"
              }
              className="fill-muted-foreground font-mono text-[10px]"
            >
              {points[index]?.day.slice(5)}
            </text>
          ))}

          {index === null ? null : (
            <g>
              <line
                x1={pointX(index, points.length)}
                x2={pointX(index, points.length)}
                y1={PAD.top}
                y2={PAD.top + PLOT_H}
                className="stroke-muted-foreground"
                strokeWidth={1}
                strokeDasharray="3 3"
              />
              {series.map(({ key, tone }) => (
                <circle
                  key={key}
                  cx={pointX(index, points.length)}
                  cy={pointY(points[index][key], max)}
                  r={3.5}
                  className={cn("fill-background stroke-current", tone)}
                  strokeWidth={2}
                />
              ))}
            </g>
          )}
        </svg>
      </CardContent>
    </Card>
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
    // page is a day, and a refetch mid-hover replaces the points under the
    // pointer.
    const timer = window.setInterval(() => void load(), 60_000);
    return () => {
      live = false;
      window.clearInterval(timer);
    };
  }, [days]);

  const totals = useMemo(
    () =>
      (points ?? []).reduce(
        (sum, point) => ({
          records: sum.records + point.records,
          games: sum.games + point.games,
          raw_bytes: sum.raw_bytes + point.raw_bytes,
        }),
        { records: 0, games: 0, raw_bytes: 0 },
      ),
    [points],
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex gap-1">
          {WINDOWS.map((window) => (
            <Button
              key={window}
              size="sm"
              variant={window === days ? "secondary" : "ghost"}
              onClick={() => setDays(window)}
            >
              {window} 天
            </Button>
          ))}
        </div>
        <p className="text-xs text-muted-foreground">
          {points
            ? `窗口内入库 ${count(totals.records)} 条 · 对局 ${count(totals.games)} 局 · 原始 ${formatBytes(totals.raw_bytes)}`
            : loading
              ? "正在读取趋势数据…"
              : "趋势数据不可用"}
        </p>
      </div>

      {error ? (
        <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
          无法读取趋势数据：{error}
        </p>
      ) : null}

      {points && points.length > 0 ? (
        <>
          <TrendChart
            points={points}
            title="入库与对局"
            description="蓝线是记录进入索引的时间，青线是这局牌实际开打的时间——一次历史导入会抬高前者而不动后者。"
            format={count}
            series={[
              { key: "records", label: "入库", tone: "text-chart-1" },
              { key: "games", label: "对局", tone: "text-chart-2" },
            ]}
          />
          <TrendChart
            points={points}
            title="每日新增数据量"
            description="按入库时间统计的牌谱体积，两条线之间的距离就是 zstd 省下的部分。"
            format={formatBytes}
            series={[
              { key: "raw_bytes", label: "原始", tone: "text-chart-1" },
              { key: "compressed_bytes", label: "压缩后", tone: "text-chart-2" },
            ]}
          />
        </>
      ) : (
        <Card className="shadow-none">
          <CardContent className="py-16 text-center text-sm text-muted-foreground">
            {loading ? "正在读取趋势数据…" : "趋势数据不可用"}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
