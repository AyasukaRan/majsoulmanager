"use client";

import { useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  NO_RULE_FILTER,
  RULE_FACETS,
  type RuleFacetKey,
} from "@/lib/rules";
import { cn } from "@/lib/utils";

/**
 * The windows a player page offers. No hourly option, unlike the trends page:
 * that one draws one bar per bucket and the granularity decides the picture,
 * where this one sums a range down to a single set of numbers and only the two
 * ends of the range matter.
 */
const WINDOWS = [
  { label: "7 天", span: 7 },
  { label: "30 天", span: 30 },
  { label: "90 天", span: 90 },
  { label: "365 天", span: 365 },
];
const DEFAULT_WINDOW = 3;

/** How long a keystroke waits before it becomes a search. */
const SEARCH_DEBOUNCE_MS = 250;

type PlayerHit = { player: string; games: number };

type Summary = {
  games: number;
  detailed_games: number;
  hands: number;
  hands_as_dealer: number;
  max_dealer_streak: number;
  net_points: number;
  placements: number[];
  busted: number;
  final_score: number;
  settled_point: number;
  grading_score: number;
  wins: number;
  wins_tsumo: number;
  win_points: number;
  win_turns: number;
  deal_ins: number;
  deal_in_points: number;
  riichi: number;
  riichi_wins: number;
  riichi_deal_ins: number;
  riichi_turns: number;
  riichi_first: number;
  riichi_chasing: number;
  riichi_chased: number;
  riichi_net: number;
  called: number;
  called_wins: number;
  draws: number;
  draws_tenpai: number;
  riichi_ippatsu: number;
  riichi_ura_hits: number;
  yakuman: number;
  max_han: number;
};

async function jsonRequest<T>(url: string): Promise<T> {
  const response = await fetch(url, { cache: "no-store" });
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(body?.error ?? `HTTP ${response.status}`);
  }
  return body as T;
}

/**
 * A rate, or an em dash when its denominator is zero.
 *
 * Rendering 0% for "never had the chance" is the one thing a statistics page
 * must not do: a player with no riichi and a player who never won one look
 * identical, and only one of them is a fact.
 */
function rate(numerator: number, denominator: number, digits = 2) {
  if (!denominator) return "—";
  return `${((100 * numerator) / denominator).toFixed(digits)}%`;
}

function mean(total: number, count: number, digits = 0) {
  if (!count) return "—";
  return (total / count).toFixed(digits);
}

function signed(value: number) {
  return `${value > 0 ? "+" : ""}${value.toLocaleString("zh-CN")}`;
}

/** Today, and `days - 1` days before it, as the reader's own calendar days. */
function calendarDay(date: Date) {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 10);
}

function dayStart(day: string) {
  return new Date(`${day}T00:00:00`).toISOString();
}

function dayEnd(day: string) {
  const end = new Date(`${day}T00:00:00`);
  end.setDate(end.getDate() + 1);
  return new Date(end.getTime() - 1).toISOString();
}

/** One label/value pair in a stats grid. */
function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <span className="text-muted-foreground text-xs">{label}</span>
      <span className="text-sm font-medium tabular-nums">{value}</span>
    </div>
  );
}

/**
 * The placement distribution, as a stacked bar rather than a pie.
 *
 * A pie of four slices is the shape Mahjong Soul's own trackers use, but the
 * question a reader actually has here — is this player heavier on first or on
 * fourth than even — is a comparison of lengths, and lengths are what a bar
 * gives. The even mark is drawn so that the comparison has something to be
 * against.
 */
function Placements({ counts, seats }: { counts: number[]; seats: number }) {
  const total = counts.reduce((sum, count) => sum + count, 0);
  const colours = [
    "bg-[var(--chart-1)]",
    "bg-[var(--chart-2)]",
    "bg-[var(--chart-3)]",
    "bg-[var(--chart-4)]",
  ];
  const names = ["一位", "二位", "三位", "四位"];
  return (
    <div className="space-y-2">
      <div className="bg-muted relative flex h-6 w-full overflow-hidden rounded-md">
        {counts.slice(0, seats).map((count, place) => (
          <div
            key={place}
            className={cn(colours[place], "h-full")}
            style={{ width: total ? `${(100 * count) / total}%` : "0%" }}
            title={`${names[place]} ${count}`}
          />
        ))}
        {/* Where the boundaries would sit if every place were equally likely.
            Without them the bar says which slice is biggest and nothing about
            whether that is unusual, which is the only question it is here to
            answer. */}
        {Array.from({ length: seats - 1 }, (_, index) => (
          <span
            key={index}
            aria-hidden
            className="bg-background/70 absolute top-0 h-full w-px"
            style={{ left: `${(100 * (index + 1)) / seats}%` }}
          />
        ))}
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1">
        {counts.slice(0, seats).map((count, place) => (
          <span key={place} className="flex items-center gap-1.5 text-xs">
            <span className={cn(colours[place], "size-2.5 rounded-[2px]")} />
            <span className="text-muted-foreground">{names[place]}</span>
            <span className="tabular-nums">{rate(count, total)}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

const TABS = ["基本", "立直", "更多"] as const;

export function PlayerStats() {
  const [query, setQuery] = useState("");
  const [hits, setHits] = useState<PlayerHit[]>([]);
  const [player, setPlayer] = useState<string | null>(null);
  const [choice, setChoice] = useState<number | "custom">(DEFAULT_WINDOW);
  const [range, setRange] = useState({ from: "", to: "" });
  const [modes, setModes] =
    useState<Record<RuleFacetKey, string[]>>(NO_RULE_FILTER);
  // Keyed by the request that produced it, so a summary is only ever shown
  // beside the player and window it was asked for. Clearing it from inside the
  // effect would be the same thing written as a state update nobody watches.
  const [answer, setAnswer] = useState<{ key: string; data: Summary } | null>(
    null,
  );
  const [tab, setTab] = useState<(typeof TABS)[number]>("基本");
  const [error, setError] = useState<string | null>(null);
  const filtered = RULE_FACETS.some((facet) => modes[facet.key].length > 0);
  const custom = choice === "custom";
  const picked = custom && range.from !== "" && range.to !== "";

  // Debounced so that typing a nickname is one search rather than one per
  // keystroke: the name column has no index a substring match can use, so every
  // one of these is a scan.
  useEffect(() => {
    let live = true;
    const timer = setTimeout(() => {
      jsonRequest<{ items: PlayerHit[] }>(
        `/api/players?q=${encodeURIComponent(query)}`,
      )
        .then((page) => {
          if (live) setHits(page.items);
        })
        .catch(() => {
          if (live) setHits([]);
        });
    }, SEARCH_DEBOUNCE_MS);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [query]);

  const search = useMemo(() => {
    if (!player || (custom && !picked)) return null;
    const params = new URLSearchParams({ player });
    if (custom) {
      params.set("unit", "day");
      params.set("from", dayStart(range.from));
      params.set("to", dayEnd(range.to));
    } else {
      params.set("unit", "day");
      params.set("span", String(WINDOWS[choice as number].span));
    }
    for (const facet of RULE_FACETS) {
      if (modes[facet.key].length > 0) {
        params.set(facet.key, modes[facet.key].join(","));
      }
    }
    return params.toString();
  }, [player, choice, custom, picked, range, modes]);

  useEffect(() => {
    if (search === null) {
      return;
    }
    let live = true;
    jsonRequest<Summary>(`/api/players/stats?${search}`)
      .then((next) => {
        if (!live) return;
        setAnswer({ key: search, data: next });
        setError(null);
      })
      .catch((problem: Error) => {
        if (!live) return;
        setError(problem.message);
      });
    return () => {
      live = false;
    };
  }, [search]);

  const toggle = (key: RuleFacetKey, value: string) =>
    setModes((previous) => ({
      ...previous,
      [key]: previous[key].includes(value)
        ? previous[key].filter((kept) => kept !== value)
        : [...previous[key], value],
    }));

  const summary = answer && answer.key === search ? answer.data : null;
  // Derived rather than a second piece of state: an answer that does not match
  // the request on screen *is* the definition of still waiting.
  const loading = search !== null && summary === null;
  // Three-player games have no fourth place, so a mix of both would draw an
  // always-empty fourth slice. Read off the placements themselves rather than
  // from a mode filter the reader may not have set.
  const seats = summary && summary.placements[3] > 0 ? 4 : 3;

  return (
    <div className="space-y-5">
      <div className="space-y-2">
        <Input
          className="h-9 max-w-md text-sm"
          placeholder="搜索玩家昵称"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          aria-label="搜索玩家昵称"
        />
        <div className="flex flex-wrap gap-1.5">
          {hits.length === 0 ? (
            <span className="text-muted-foreground text-xs">
              {query ? "没有匹配的玩家" : "正在读取玩家列表"}
            </span>
          ) : (
            hits.map((hit) => (
              <Button
                key={hit.player}
                size="sm"
                variant="ghost"
                aria-pressed={hit.player === player}
                className={cn(
                  "h-7 px-2.5 text-xs",
                  hit.player === player
                    ? "bg-primary text-primary-foreground hover:bg-primary"
                    : "bg-muted text-muted-foreground",
                )}
                onClick={() => setPlayer(hit.player)}
              >
                {hit.player}
                <span className="ml-1.5 tabular-nums opacity-70">
                  {hit.games}
                </span>
              </Button>
            ))
          )}
        </div>
      </div>

      {player ? (
        <>
          <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
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
                  if (!custom) {
                    const today = new Date();
                    const start = new Date(today);
                    start.setDate(start.getDate() - 29);
                    setRange({
                      from: calendarDay(start),
                      to: calendarDay(today),
                    });
                  }
                  setChoice("custom");
                }}
              >
                自定义
              </Button>
            </div>
            {RULE_FACETS.map((facet) => (
              <div key={facet.key} className="flex items-center gap-2">
                <span className="text-muted-foreground text-xs">
                  {facet.label}
                </span>
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
                  setRange((previous) => ({
                    ...previous,
                    to: event.target.value,
                  }))
                }
              />
              {!picked ? (
                <span className="text-muted-foreground">
                  选好起止两天才会查询
                </span>
              ) : null}
            </div>
          ) : null}
        </>
      ) : null}

      {error ? (
        <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
          无法读取玩家战绩：{error}
        </p>
      ) : null}

      {player && summary && summary.games === 0 && !loading ? (
        <p className="text-muted-foreground text-sm">
          这个窗口和筛选下没有对局。
        </p>
      ) : null}

      {player && summary && summary.games > 0 ? (
        <>
          <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,20rem)]">
            <div className="space-y-4 rounded-xl border p-4">
              <div className="flex flex-wrap items-baseline gap-x-6 gap-y-1">
                <span className="text-2xl font-semibold tracking-tight">
                  {player}
                </span>
                <span className="text-muted-foreground text-xs">
                  {summary.games.toLocaleString("zh-CN")} 场 ·{" "}
                  {summary.hands.toLocaleString("zh-CN")} 局 · 平均顺位{" "}
                  {mean(
                    summary.placements.reduce(
                      (sum, count, place) => sum + count * (place + 1),
                      0,
                    ),
                    summary.games,
                    3,
                  )}
                </span>
              </div>
              <div className="bg-muted inline-flex rounded-lg p-[3px]">
                {TABS.map((name) => (
                  <Button
                    key={name}
                    size="sm"
                    variant="ghost"
                    aria-pressed={name === tab}
                    className={cn(
                      "h-7 px-3 text-xs",
                      name === tab
                        ? "bg-background text-foreground hover:bg-background shadow-sm"
                        : "text-muted-foreground",
                    )}
                    onClick={() => setTab(name)}
                  >
                    {name}
                  </Button>
                ))}
              </div>
              <div className="grid gap-x-8 gap-y-2 sm:grid-cols-2 xl:grid-cols-3">
                {tab === "基本" ? (
                  <>
                    <Stat label="和牌率" value={rate(summary.wins, summary.hands)} />
                    <Stat label="放铳率" value={rate(summary.deal_ins, summary.hands)} />
                    <Stat label="自摸率" value={rate(summary.wins_tsumo, summary.wins)} />
                    <Stat label="副露率" value={rate(summary.called, summary.hands)} />
                    <Stat label="立直率" value={rate(summary.riichi, summary.hands)} />
                    <Stat label="流局率" value={rate(summary.draws, summary.hands)} />
                    <Stat
                      label="平均打点"
                      value={mean(summary.win_points, summary.wins)}
                    />
                    <Stat
                      label="平均铳点"
                      value={mean(summary.deal_in_points, summary.deal_ins)}
                    />
                    <Stat
                      label="和了巡数"
                      value={mean(summary.win_turns, summary.wins, 3)}
                    />
                    <Stat label="被飞率" value={rate(summary.busted, summary.games)} />
                    <Stat
                      label="局收支"
                      value={
                        summary.hands
                          ? signed(Math.round(summary.net_points / summary.hands))
                          : "—"
                      }
                    />
                    <Stat label="最大连庄" value={String(summary.max_dealer_streak)} />
                  </>
                ) : null}
                {tab === "立直" ? (
                  <>
                    <Stat label="立直率" value={rate(summary.riichi, summary.hands)} />
                    <Stat
                      label="立直和了"
                      value={rate(summary.riichi_wins, summary.riichi)}
                    />
                    <Stat
                      label="立直放铳"
                      value={rate(summary.riichi_deal_ins, summary.riichi)}
                    />
                    <Stat
                      label="立直巡目"
                      value={mean(summary.riichi_turns, summary.riichi, 3)}
                    />
                    <Stat
                      label="先制率"
                      value={rate(summary.riichi_first, summary.riichi)}
                    />
                    <Stat
                      label="追立率"
                      value={rate(summary.riichi_chasing, summary.riichi)}
                    />
                    <Stat
                      label="被追率"
                      value={rate(summary.riichi_chased, summary.riichi)}
                    />
                    <Stat
                      label="立直收支"
                      value={
                        summary.riichi
                          ? signed(Math.round(summary.riichi_net / summary.riichi))
                          : "—"
                      }
                    />
                    <Stat
                      label="一发率"
                      value={
                        summary.detailed_games
                          ? rate(summary.riichi_ippatsu, summary.riichi)
                          : "—"
                      }
                    />
                    <Stat
                      label="里宝率"
                      value={
                        summary.detailed_games
                          ? rate(summary.riichi_ura_hits, summary.riichi_wins)
                          : "—"
                      }
                    />
                  </>
                ) : null}
                {tab === "更多" ? (
                  <>
                    <Stat label="总计局数" value={summary.hands.toLocaleString("zh-CN")} />
                    <Stat
                      label="做庄局数"
                      value={summary.hands_as_dealer.toLocaleString("zh-CN")}
                    />
                    <Stat
                      label="副露后和牌"
                      value={rate(summary.called_wins, summary.called)}
                    />
                    <Stat
                      label="流听率"
                      value={
                        summary.detailed_games
                          ? rate(summary.draws_tenpai, summary.draws)
                          : "—"
                      }
                    />
                    <Stat
                      label="役满"
                      value={summary.detailed_games ? String(summary.yakuman) : "—"}
                    />
                    <Stat
                      label="最大番数"
                      value={summary.detailed_games ? String(summary.max_han) : "—"}
                    />
                    <Stat
                      label="平均精算"
                      value={
                        summary.detailed_games
                          ? signed(
                              Math.round(summary.settled_point / summary.detailed_games),
                            )
                          : "—"
                      }
                    />
                    <Stat
                      label="段位分变动"
                      value={
                        summary.detailed_games ? signed(summary.grading_score) : "—"
                      }
                    />
                    <Stat
                      label="平均终局点数"
                      value={mean(summary.final_score, summary.games)}
                    />
                  </>
                ) : null}
              </div>
            </div>

            <div className="space-y-3 rounded-xl border p-4">
              <h2 className="text-sm font-medium">顺位分布</h2>
              <Placements counts={summary.placements} seats={seats} />
            </div>
          </section>

          {summary.detailed_games < summary.games ? (
            <p className="text-muted-foreground text-xs">
              一发率、里宝率、流听率、役满、最大番数和精算只能从带记分明细的记录算出来，
              这个窗口里有 {summary.detailed_games.toLocaleString("zh-CN")} 场 /{" "}
              {summary.games.toLocaleString("zh-CN")} 场是这种记录；更早转换的记录没有这些字段。
            </p>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
