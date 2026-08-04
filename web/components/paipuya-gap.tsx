"use client";

import { useCallback, useEffect, useState } from "react";
import { LibraryBig, RefreshCw } from "lucide-react";

import { jsonRequest, type PaipuyaReport } from "@/lib/mjai-api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { selectClass } from "@/lib/utils";

/** The same presets the trend charts offer, in the units the API takes. */
const WINDOWS: Array<{ label: string; unit: "hour" | "day"; span: number }> = [
  { label: "最近 7 天", unit: "day", span: 7 },
  { label: "最近 30 天", unit: "day", span: 30 },
  { label: "最近 90 天", unit: "day", span: 90 },
  { label: "最近 365 天", unit: "day", span: 365 },
];

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

function day(value: string | null) {
  return value ? new Date(value).toLocaleDateString("zh-CN") : "—";
}

/**
 * What 牌谱屋 lists against what this corpus holds.
 *
 * The comparison is on start time and player names, not uuids — so this answers
 * "how many games are we missing" without spending a single Mahjong Soul
 * request, which is the whole reason it runs before any fetching.
 */
export function PaipuyaGapCard() {
  const [report, setReport] = useState<PaipuyaReport | null>(null);
  const [window_, setWindow] = useState(WINDOWS[1]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async (chosen: (typeof WINDOWS)[number]) => {
    setLoading(true);
    try {
      const query = new URLSearchParams({
        unit: chosen.unit,
        span: String(chosen.span),
      });
      setReport(
        await jsonRequest<PaipuyaReport>(`/api/paipuya/gap?${query.toString()}`),
      );
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "读取缺口失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(window_), 0);
    return () => window.clearTimeout(initial);
  }, [refresh, window_]);

  const covered =
    report && report.listed > 0
      ? ((report.listed - report.missing) / report.listed) * 100
      : null;

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="gap-4">
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
          <div>
            <CardTitle className="flex items-center gap-2">
              <LibraryBig className="size-4" />
              牌谱屋对照
              {report ? (
                <Badge variant="outline" className="font-mono">
                  已同步 {count(report.games)} 局
                </Badge>
              ) : null}
            </CardTitle>
            <CardDescription className="mt-1">
              {error
                ? error
                : report && report.games > 0
                  ? `同步范围 ${day(report.earliest)} — ${day(report.latest)}；按开局时间和玩家比对，不用 uuid`
                  : "还没有同步过牌谱屋的对局信息"}
            </CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <select
              className={`${selectClass} w-auto`}
              value={window_.label}
              onChange={(event) =>
                setWindow(
                  WINDOWS.find((item) => item.label === event.target.value) ??
                    WINDOWS[1],
                )
              }
            >
              {WINDOWS.map((item) => (
                <option key={item.label} value={item.label}>
                  {item.label}
                </option>
              ))}
            </select>
            <Button
              variant="outline"
              size="sm"
              className="gap-2"
              onClick={() => void refresh(window_)}
              disabled={loading}
            >
              <RefreshCw className={`size-3.5 ${loading ? "animate-spin" : ""}`} />
              比对
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-2 md:grid-cols-3">
          <div className="rounded-lg border bg-muted/25 px-3 py-2.5">
            <p className="text-xs text-muted-foreground">窗口内牌谱屋收录</p>
            <p className="mt-1 font-mono text-xl font-semibold">
              {report ? count(report.listed) : "—"}
            </p>
          </div>
          <div className="rounded-lg border bg-muted/25 px-3 py-2.5">
            <p className="text-xs text-muted-foreground">本地已有</p>
            <p className="mt-1 font-mono text-xl font-semibold">
              {report ? count(report.listed - report.missing) : "—"}
            </p>
          </div>
          <div className="rounded-lg border bg-muted/25 px-3 py-2.5">
            <p className="text-xs text-muted-foreground">
              缺口{covered === null ? "" : `（覆盖 ${covered.toFixed(1)}%）`}
            </p>
            <p className="mt-1 font-mono text-xl font-semibold">
              {report ? count(report.missing) : "—"}
            </p>
          </div>
        </div>
        <p className="mt-3 text-xs leading-relaxed text-muted-foreground">
          对局信息用{" "}
          <code className="font-mono">POST /api/v1/paipuya/games</code>{" "}
          灌进来（一批最多 100,000 局）。这里只做比对，不发任何雀魂请求——只有比对不上的那些才值得去取
          uuid 抓回来。
        </p>
      </CardContent>
    </Card>
  );
}
