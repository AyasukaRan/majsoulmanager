"use client";

import { useCallback, useEffect, useState } from "react";
import {
  CalendarRange,
  CirclePlay,
  CircleStop,
  KeyRound,
  RotateCcw,
  Save,
  Timer,
} from "lucide-react";

import {
  ApiRequestError,
  jsonRequest,
  type PaipuyaConfig,
  type PaipuyaStatus,
} from "@/lib/mjai-api";
import { day, windowPercent } from "@/lib/paipuya-progress.mjs";
import { ruleLabel } from "@/lib/rules";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { useBusyAction } from "@/components/watch/busy-action";

const PHASE_LABELS: Record<PaipuyaStatus["phase"], string> = {
  stopped: "已停止",
  starting: "启动中",
  running: "同步中",
  reloading: "重新加载中",
  stopping: "停止中",
  failed: "失败",
};

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

/**
 * The 牌谱屋 sync: where to pull from, with what key, and how often.
 *
 * The interval is a floor on requests leaving the backend counting every worker
 * together, not a per-worker sleep — so raising 并发 does not raise the rate. It
 * only stops one slow response from wasting the allowance. Getting that
 * distinction wrong on somebody else's free service is how a key gets revoked.
 */
export function PaipuyaSyncCard() {
  const [config, setConfig] = useState<PaipuyaConfig | null>(null);
  const [status, setStatus] = useState<PaipuyaStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { busy, message, run } = useBusyAction();

  const loadConfig = useCallback(async () => {
    try {
      setConfig(await jsonRequest<PaipuyaConfig>("/api/paipuya/config"));
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "读取同步配置失败");
    }
  }, []);

  const loadStatus = useCallback(async () => {
    try {
      setStatus(await jsonRequest<PaipuyaStatus>("/api/paipuya/status"));
    } catch {
      // Polled; one failed read is not worth a banner.
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => {
      void loadConfig();
      void loadStatus();
    }, 0);
    const timer = window.setInterval(() => void loadStatus(), 5_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadConfig, loadStatus]);

  async function save() {
    if (!config) return;
    await run("保存失败", async () => {
      try {
        const saved = await jsonRequest<PaipuyaConfig>("/api/paipuya/config", {
          method: "PUT",
          body: JSON.stringify(config),
        });
        setConfig(saved);
        return `配置 r${saved.revision} 已保存`;
      } catch (failure) {
        if (failure instanceof ApiRequestError && failure.status === 412) {
          await loadConfig();
          throw new Error("配置已被其他人修改，已重新载入，请重新编辑后保存");
        }
        throw failure;
      }
    });
  }

  async function action(next: "start" | "stop") {
    await run("操作失败", async () => {
      await jsonRequest("/api/paipuya/actions", {
        method: "POST",
        body: JSON.stringify({ action: next }),
      });
      await loadStatus();
      return next === "start" ? "同步已启动" : "同步已停止";
    });
  }

  /**
   * Needed whenever the window moves to somewhere a cursor has already passed:
   * 开始时间 pulled backwards, or an 结束时间 set in the past. Both leave the
   * mode with nothing between the two edges, so it fetches nothing at all —
   * the amber rows in the list are exactly those.
   *
   * Moving 开始时间 *forward* needs none of this; the sweep clamps a lagging
   * cursor up to the window on its own. This button is for the case that costs
   * a full re-sweep, which is why it is a button and not a side effect of
   * saving.
   */
  async function resetCursors() {
    await run("重置失败", async () => {
      const status = await jsonRequest<PaipuyaStatus>(
        "/api/paipuya/cursors/reset",
        { method: "POST" },
      );
      setStatus(status);
      return "游标已退回开始时间；同步会停下，重新点「开始同步」";
    });
  }

  if (!config) {
    return (
      <Card className="border-border/70 shadow-none">
        <CardContent className="flex h-28 items-center justify-center text-sm text-muted-foreground">
          {error ?? "正在读取牌谱屋同步配置…"}
        </CardContent>
      </Card>
    );
  }

  const progress = status?.progress;
  const rate = (1000 / config.request_interval_ms).toFixed(2);
  const modes = status?.modes ?? [];
  // From the status rather than from the edit in hand: an unsaved change to the
  // window must not redraw the bars as though it had taken effect.
  const windowStart = Date.parse(status?.window_start ?? config.start_from);
  // The backend's clock, not this browser's: the cursors are stamped against
  // it, and it keeps the bars from moving because a laptop's time is off.
  const windowEnd = Date.parse(
    status?.window_end ?? status?.now ?? config.start_from,
  );
  const catalogued = modes.reduce((sum, mode) => sum + mode.synced_games, 0);

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="border-b">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <CardTitle className="flex flex-wrap items-center gap-2">
              <KeyRound className="size-4" />
              牌谱屋同步
              <Badge variant="outline">
                {PHASE_LABELS[status?.phase ?? "stopped"]}
              </Badge>
              {status && !status.has_key ? (
                <Badge
                  variant="outline"
                  className="border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300"
                >
                  未配置 key
                </Badge>
              ) : null}
            </CardTitle>
            <CardDescription className="mt-1">
              {status?.last_error ??
                (progress
                  ? `本次已同步 ${count(progress.synced)} 局，请求 ${count(progress.requests)} 次，被拒 ${count(progress.refused)} 次`
                  : "从牌谱屋按时间窗拉取对局信息，只写目录，不抓牌谱")}
            </CardDescription>
          </div>
          <Badge variant="outline" className="font-mono">
            r{config.revision}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-5">
        <div className="grid gap-3 sm:grid-cols-2">
          <label className="space-y-1.5 text-xs font-medium">
            接口地址
            <Input
              value={config.base_url}
              onChange={(event) =>
                setConfig({ ...config, base_url: event.target.value })
              }
            />
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            API key
            <Input
              value={config.api_key ?? ""}
              placeholder="Bearer …（向 SAPikachu 申请）"
              onChange={(event) =>
                setConfig({ ...config, api_key: event.target.value || null })
              }
            />
            <span className="block font-normal text-muted-foreground">
              读出来永远是 <code className="font-mono">***</code>
              ，原样存回去不会覆盖已存的那把；要换就直接输入新的。默认放进{" "}
              <code className="font-mono">{config.api_key_header}</code> 头，值原样发送。
            </span>
          </label>
        </div>

        <div className="grid gap-3 sm:grid-cols-2">
          <label className="space-y-1.5 text-xs font-medium">
            <span className="flex items-center gap-1.5">
              <CalendarRange className="size-3.5" />
              同步开始时间
            </span>
            <Input
              type="date"
              value={day(config.start_from)}
              onChange={(event) =>
                setConfig({
                  ...config,
                  // An empty date box keeps the stored edge rather than
                  // becoming the epoch: clearing it is not a thing an operator
                  // can mean here, the window has to start somewhere.
                  start_from: event.target.value
                    ? `${event.target.value}T00:00:00Z`
                    : config.start_from,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              牌谱屋从 2019-08-23 起收录。往后调立刻生效；往前调要先「重置游标」。
            </span>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            同步结束时间
            <Input
              type="date"
              value={config.sync_until ? day(config.sync_until) : ""}
              onChange={(event) =>
                setConfig({
                  ...config,
                  sync_until: event.target.value
                    ? `${event.target.value}T00:00:00Z`
                    : null,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              留空就是一直跟到最新。填了之后每个模式走到这天就停。填一个游标已经走过的日期，那些模式什么都不会抓——先「重置游标」。
            </span>
          </label>
        </div>

        <div className="grid gap-3 sm:grid-cols-3">
          <label className="space-y-1.5 text-xs font-medium">
            <span className="flex items-center gap-1.5">
              <Timer className="size-3.5" />
              请求间隔（毫秒）
            </span>
            <Input
              type="number"
              min={200}
              step={100}
              value={config.request_interval_ms}
              onChange={(event) =>
                setConfig({
                  ...config,
                  request_interval_ms: Number(event.target.value) || 1000,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              全局下限，约 {rate} qps。并发不会抬高它。
            </span>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            并发
            <Input
              type="number"
              min={1}
              max={8}
              value={config.concurrency}
              onChange={(event) =>
                setConfig({
                  ...config,
                  concurrency: Number(event.target.value) || 1,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              只是不让一次慢响应白占额度
            </span>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            每页条数
            <Input
              type="number"
              min={1}
              max={500}
              value={config.page_size}
              onChange={(event) =>
                setConfig({
                  ...config,
                  page_size: Number(event.target.value) || 500,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              牌谱屋上限 500
            </span>
          </label>
        </div>

        {/* Time is the only honest measure of this sweep. How many games the
            catalogue holds for a window is not known until the window has been
            swept, so a percentage of games would be a percentage of a number
            nobody has — but the window itself is exact, and a cursor is a point
            in it. */}
        <div className="space-y-2 rounded-lg border bg-muted/25 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs font-medium">
              同步进度
              <span className="ml-2 font-normal text-muted-foreground">
                {modes.length > 0
                  ? `${modes.length} 个模式已开始，目录累计 ${count(catalogued)} 局`
                  : "还没有任何模式开始"}
              </span>
            </p>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void resetCursors()}
              disabled={busy || modes.length === 0}
            >
              <RotateCcw className="size-3.5" />
              重置游标
            </Button>
          </div>
          {modes.map((mode) => {
            const percent = windowPercent(
              mode.next_from,
              windowStart,
              windowEnd,
            );
            const label = ruleLabel(mode.rule);
            // A cursor past the right edge is clamped to a full bar, and a full
            // bar means finished — which is the one thing it is not. It happens
            // when the window moves behind a mode that had already swept past
            // it, and that mode will fetch nothing at all until it is rewound.
            const beyond = Date.parse(mode.next_from) > windowEnd;
            return (
              <div
                key={mode.mode_id}
                // The right column is a fixed width, not `auto`. Sized to
                // content it grew with the number in it, which shortened that
                // row's track — and a bar you cannot compare against the bar
                // above it is not doing the one job a bar has.
                className="grid grid-cols-[minmax(0,8.5rem)_1fr] items-center gap-x-3 gap-y-1 sm:grid-cols-[minmax(0,8.5rem)_1fr_12rem]"
              >
                <span className="truncate text-xs">{label}</span>
                <div
                  role="progressbar"
                  aria-label={
                    beyond ? `${label} 游标在窗口之后` : `${label} 同步进度`
                  }
                  aria-valuenow={Math.round(percent)}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  className="h-1.5 overflow-hidden rounded-full bg-muted"
                >
                  <div
                    className={cn(
                      "h-full transition-all",
                      beyond ? "bg-amber-500" : "bg-primary",
                    )}
                    style={{ width: `${percent}%` }}
                  />
                </div>
                <span
                  className={cn(
                    "text-xs tabular-nums sm:text-right",
                    beyond ? "text-amber-700" : "text-muted-foreground",
                  )}
                >
                  {beyond
                    ? `${day(mode.next_from)} · 在窗口之后`
                    : `${day(mode.next_from)} · ${count(mode.synced_games)} 局`}
                </span>
              </div>
            );
          })}
          <p className="text-xs text-muted-foreground">
            按时间算：{day(config.start_from)} →{" "}
            {config.sync_until ? day(config.sync_until) : "现在"}
            。目录累计是历次同步的总和，卡片顶上那行是本次运行的。
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-4 rounded-lg border bg-muted/25 p-3">
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={config.enabled}
              onCheckedChange={(checked) =>
                setConfig({ ...config, enabled: checked })
              }
            />
            随后端启动
          </label>
          <span className="text-xs text-muted-foreground">
            没有 key 时牌谱屋的额度极低，同步基本会一直被 429 拒
          </span>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void save()} disabled={busy}>
            <Save className="size-4" />
            保存并应用
          </Button>
          <Button
            variant="outline"
            onClick={() => void action("start")}
            disabled={busy}
          >
            <CirclePlay className="size-4" />
            开始同步
          </Button>
          <Button
            variant="outline"
            onClick={() => void action("stop")}
            disabled={busy}
          >
            <CircleStop className="size-4" />
            停止
          </Button>
        </div>
        {message ? (
          <p className="rounded-lg border bg-background px-3 py-2 text-xs text-muted-foreground">
            {message}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}
