"use client";

import { useCallback, useEffect, useState } from "react";
import {
  CirclePlay,
  CircleStop,
  Gauge,
  KeyRound,
  RefreshCw,
  Save,
  Timer,
} from "lucide-react";

import {
  ApiRequestError,
  jsonRequest,
  type RefetchServiceConfig,
  type RefetchStatus,
} from "@/lib/mjai-api";
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
import { selectClass } from "@/lib/utils";
import { useBusyAction } from "@/components/watch/busy-action";

const PHASE_LABELS: Record<RefetchStatus["phase"], string> = {
  stopped: "已停止",
  starting: "启动中",
  running: "运行中",
  reloading: "重新加载中",
  stopping: "停止中",
  failed: "失败",
};

function phaseClass(phase: RefetchStatus["phase"]) {
  switch (phase) {
    case "running":
      return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-300";
    case "starting":
    case "reloading":
    case "stopping":
      return "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300";
    case "failed":
      return "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300";
    default:
      return "border-border bg-muted text-muted-foreground";
  }
}

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

/** Where the sweep has read to. A date, because the catalogue is time ordered. */
function moment(value: string) {
  return new Date(value).toLocaleString("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

/**
 * Live numbers for a run.
 *
 * Two shapes, because the two backlogs are not the same kind of thing. The pb
 * repair has an end: `backlog` is read once when a run starts, so the bar
 * measures this run against the work it set out to do rather than against a
 * figure that moves under it, and records the walk skipped stay in it forever,
 * which is why the bar is not the only thing shown. The 牌谱屋 sweep has no end
 * to measure against, so it reports where in the catalogue it has read to.
 */
function RefetchStatusCard({
  status,
  work,
  onRefresh,
}: {
  status: RefetchStatus | null;
  work: RefetchServiceConfig["work"];
  onRefresh: () => void;
}) {
  const progress = status?.progress;
  const backlog = status?.backlog ?? null;
  const done = progress?.replaced ?? 0;
  const sweeping = work === "known_games";
  // No bar for the sweep, and not because one is hard to draw. 牌谱屋 lists
  // three orders of magnitude more games than this corpus holds, so a
  // percentage would be this run's fetches over a number no year of fetching
  // approaches — 0.0% for the life of the deployment. What the walk actually
  // has is a position in the catalogue, and that is what is shown instead.
  const percent =
    !sweeping && backlog && backlog > 0
      ? Math.min(100, (done / backlog) * 100)
      : null;

  const tiles = sweeping
    ? [
        { label: "已走查", value: progress?.scanned ?? 0 },
        { label: "本地已有", value: progress?.present ?? 0 },
        { label: "已入库", value: done },
        { label: "雀魂拒绝", value: progress?.refused ?? 0 },
        { label: "转换失败", value: progress?.unconvertible ?? 0 },
        // Should stay near zero: a game only reaches here if its claim landed
        // after its page was compared. A number that climbs means requests are
        // being spent on games the corpus already has.
        { label: "抓回来是重复", value: progress?.duplicates ?? 0 },
      ]
    : [
        // What is left, not what there was. The bar above already states the
        // figure the run started from, and a tile that repeated it beside a bar
        // reading 31% looked like the two disagreed.
        {
          label: "剩余待补",
          value: backlog === null ? null : Math.max(0, backlog - done),
        },
        { label: "已替换", value: done },
        { label: "已扫描", value: progress?.scanned ?? 0 },
        { label: "雀魂拒绝", value: progress?.refused ?? 0 },
        { label: "无法替换", value: progress?.unconvertible ?? 0 },
        { label: "读不到字节", value: progress?.unreadable ?? 0 },
      ];

  // Only the causes that happened. Listing all four with three zeros reads as
  // "four things went wrong" at a glance.
  const by = progress?.unconvertible_by;
  const breakdown = !by
    ? []
    : (
        [
          ["库里读不出 uuid", by.no_uuid],
          ["转换器读不了", by.convert_failed],
          ["抓回来是另一局", by.wrong_game],
          ["重转的不完整", by.truncated],
        ] as const
      ).filter(([, n]) => n > 0);

  return (
    // No `border-b` on the header: this card is all header, and the rule drew a
    // divider with nothing under it.
    <Card className="border-border/70 shadow-none">
      <CardHeader className="gap-4">
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle>补抓进度</CardTitle>
              <Badge
                variant="outline"
                className={phaseClass(status?.phase ?? "stopped")}
              >
                {PHASE_LABELS[status?.phase ?? "stopped"]}
              </Badge>
              {status && status.phase === "running" ? (
                <Badge variant="outline" className="font-mono">
                  会话 {status.sessions}/{status.workers}
                </Badge>
              ) : null}
              {status && status.phase === "running" ? (
                <Badge variant="outline" className="font-mono">
                  {status.qps >= 10
                    ? Math.round(status.qps)
                    : status.qps.toFixed(1)}{" "}
                  条/秒
                </Badge>
              ) : null}
              {status && status.waiting > 0 ? (
                <Badge variant="outline" className="font-mono">
                  排队 {status.waiting}
                </Badge>
              ) : null}
            </div>
            <CardDescription className="mt-1">
              {status?.last_error
                ? status.last_error
                : status && status.progress.pass > 0
                  ? `第 ${status.progress.pass} 轮 · 可用账号 ${status.accounts} 个${
                      progress?.position
                        ? ` · 已走到 ${moment(progress.position)}`
                        : ""
                    }`
                  : sweeping
                    ? "按开局时间走查牌谱屋的收录，本地没有的才按 uuid 抓回来"
                    : "按 uuid 重新抓取雀魂原始牌谱，用当前转换器重转并替换索引里那一行"}
            </CardDescription>
          </div>
          <Button variant="outline" size="sm" className="gap-2" onClick={onRefresh}>
            <RefreshCw className="size-3.5" />
            刷新
          </Button>
        </div>
        {percent === null ? null : (
          <div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>本轮开始时待补 {count(backlog ?? 0)} 条</span>
              <span className="font-mono">{percent.toFixed(1)}%</span>
            </div>
            <div className="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${percent}%` }}
              />
            </div>
          </div>
        )}
        <div className="grid grid-cols-2 gap-2 md:grid-cols-3 xl:grid-cols-6">
          {tiles.map(({ label, value }) => (
            <div key={label} className="rounded-lg border bg-muted/25 px-3 py-2.5">
              <p className="text-xs text-muted-foreground">{label}</p>
              <p className="mt-1 font-mono text-xl font-semibold">
                {value === null ? "—" : count(value)}
              </p>
            </div>
          ))}
        </div>
        {breakdown.length === 0 ? null : (
          <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
            <span>无法替换的原因：</span>
            {breakdown.map(([label, n]) => (
              <span key={label} className="rounded border bg-muted/25 px-2 py-0.5">
                {label} <span className="font-mono font-semibold">{count(n)}</span>
              </span>
            ))}
          </div>
        )}
        {!status || status.failures.length === 0 ? null : (
          <div className="rounded-lg border">
            <div className="flex items-center justify-between border-b px-3 py-2">
              <p className="text-xs font-medium">最近替换不了的记录</p>
              <p className="text-xs text-muted-foreground">
                只留最近 {status.failures.length} 条，完整记录看服务日志
              </p>
            </div>
            <div className="max-h-64 overflow-y-auto font-mono text-xs">
              {status.failures.map((f, i) => (
                <div
                  key={`${f.at}-${f.subject}-${i}`}
                  className="flex gap-2 border-b px-3 py-1.5 last:border-b-0"
                >
                  <span className="shrink-0 text-muted-foreground">
                    {new Date(f.at).toLocaleTimeString("zh-CN", { hour12: false })}
                  </span>
                  <span className="shrink-0 text-red-700 dark:text-red-300">
                    {f.label}
                  </span>
                  <span className="shrink-0 text-muted-foreground">{f.subject}</span>
                  <span className="truncate text-muted-foreground">{f.detail}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardHeader>
    </Card>
  );
}

function RefetchConfigCard({
  config,
  onChange,
  status,
  onReload,
}: {
  config: RefetchServiceConfig;
  onChange: (next: RefetchServiceConfig) => void;
  status: RefetchStatus | null;
  onReload: () => Promise<void>;
}) {
  const { busy, message, run } = useBusyAction();

  async function save() {
    await run("保存失败", async () => {
      try {
        const saved = await jsonRequest<RefetchServiceConfig>(
          "/api/refetch/config",
          { method: "PUT", body: JSON.stringify(config) },
        );
        onChange(saved);
        return `配置 r${saved.revision} 已保存${saved.enabled ? "并启动" : "，服务已停止"}`;
      } catch (error) {
        // Same rule the watch configuration follows: a document is saved whole,
        // so an edit built on a revision that has moved on would silently undo
        // whatever was changed since. Only this one status pulls the current
        // document back down; every other failure means the edit in hand is
        // still the right one to retry.
        if (error instanceof ApiRequestError && error.status === 412) {
          await onReload();
          throw new Error("配置已被其他人修改，已重新载入，请重新编辑后保存");
        }
        throw error;
      }
    });
  }

  async function action(next: "start" | "stop") {
    await run("操作失败", async () => {
      await jsonRequest("/api/refetch/actions", {
        method: "POST",
        body: JSON.stringify({ action: next }),
      });
      return next === "start" ? "补抓已启动" : "补抓已停止";
    });
  }

  const capped =
    status !== null && status.workers > 0 && status.workers < config.concurrency;

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="border-b">
        <div className="flex items-start justify-between gap-4">
          <div>
            <CardTitle className="flex items-center gap-2">
              <KeyRound className="size-4" />
              账号池与节流
            </CardTitle>
            <CardDescription className="mt-1">
              保存后立即生效，不重启管理 API
            </CardDescription>
          </div>
          <Badge variant="outline" className="font-mono">
            r{config.revision}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-5">
        <label className="block space-y-1.5 text-xs font-medium">
          抓什么
          <select
            className={selectClass}
            value={config.work}
            onChange={(event) =>
              onChange({
                ...config,
                work: event.target.value as RefetchServiceConfig["work"],
              })
            }
          >
            <option value="missing_pb">补索引里缺的原始牌谱</option>
            <option value="known_games">抓已知 uuid、本地没有的对局</option>
          </select>
          <span className="block font-normal text-muted-foreground">
            {config.work === "known_games"
              ? "按开局时间走查 mjai.game_uuids 里的完整对局 uuid（majsoul2mjai 枚举解析出来的那批），每页先查一次「这局存过没有」，只有没存过的才发雀魂请求。走到哪存在库里，重启接着走；走完一遍回到开头重试被拒的。清单有一亿九千多万条，这个走查不会「跑完」。走的不是上面同步下来的牌谱屋收录——那边给的是 11 位短 id，雀魂不认。"
              : "重新抓取转换器修好之前入库那批记录的原始牌谱，替换索引里那一行。这一批是有限的，抓完就停。"}
          </span>
        </label>

        {config.work === "known_games" ? (
          <label className="block space-y-1.5 text-xs font-medium">
            从哪一天开始走
            {/* Only the field is narrow. Constraining the label narrowed the
                note under it too, into a column half the width of every other
                note on the page. */}
            <Input
              className="sm:max-w-[220px]"
              type="date"
              value={config.sweep_from?.slice(0, 10) ?? ""}
              onChange={(event) =>
                onChange({
                  ...config,
                  sweep_from: event.target.value
                    ? `${event.target.value}T00:00:00Z`
                    : null,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              {"只在没有走查位置时用一次，之后以库里存的位置为准。uuid 清单从 2019 年起，本地语料从 2026-07 才开始——不填就是从 2019 走起，那一段本地一局都没有、也没人量过雀魂还给不给这么早的牌谱，额度会先花在那里。"}
            </span>
          </label>
        ) : null}

        <div className="grid gap-3 sm:grid-cols-[180px_1fr]">
          <label className="space-y-1.5 text-xs font-medium">
            服务器
            <select
              className={selectClass}
              value={config.server}
              onChange={(event) =>
                onChange({
                  ...config,
                  server: event.target.value as RefetchServiceConfig["server"],
                })
              }
            >
              <option value="cn">国服</option>
              <option value="en">国际服</option>
              <option value="jp">日服</option>
            </select>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            账号池
            <Input
              value={config.account_secret_ref}
              onChange={(event) =>
                onChange({ ...config, account_secret_ref: event.target.value })
              }
              placeholder="pool:refetch"
            />
          </label>
        </div>
        {/* Plain text, not the bordered note box used elsewhere: sitting
            directly under an input, a bordered box reads as a second field. */}
        <p className="text-xs leading-relaxed text-muted-foreground">
          <code className="font-mono">pool:refetch</code>{" "}
          用控制台「账号池」页里填的那批；也可以继续写{" "}
          <code className="font-mono">file:</code> 或{" "}
          <code className="font-mono">env:</code> 引用，一行一个{" "}
          <code className="font-mono">账号,密码</code>。
          {"密码不进配置文档也不进接口。采集实例已经占用的账号会被自动剔除——雀魂一个账号只能开一个会话，抢账号会把实时采集踢下线。"}
        </p>

        <div className="grid gap-3 sm:grid-cols-[180px_1fr]">
          <label className="space-y-1.5 text-xs font-medium">
            补抓出站
            <select
              className={selectClass}
              value={config.proxy_mode}
              onChange={(event) =>
                onChange({
                  ...config,
                  proxy_mode: event.target
                    .value as RefetchServiceConfig["proxy_mode"],
                })
              }
            >
              <option value="mihomo">mihomo 策略组</option>
              <option value="direct">直连</option>
              <option value="custom">自定义代理</option>
            </select>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            自定义代理 URL
            <Input
              value={config.custom_proxy_url ?? ""}
              disabled={config.proxy_mode !== "custom"}
              onChange={(event) =>
                onChange({
                  ...config,
                  custom_proxy_url: event.target.value || null,
                })
              }
              placeholder="http://proxy.example:7890"
            />
          </label>
        </div>

        <div className="grid gap-3 sm:grid-cols-2">
          <label className="space-y-1.5 text-xs font-medium">
            <span className="flex items-center gap-1.5">
              <Gauge className="size-3.5" />
              并发会话数
            </span>
            {/* No maximum. The old 16 was a typo guard, not a Mahjong Soul
                limit, and a pool of eighty accounts spent sixty-four of them
                idle because of it. What actually bounds the sessions is the
                account count, which the line below this box reports. */}
            <Input
              type="number"
              min={1}
              value={config.concurrency}
              onChange={(event) =>
                onChange({
                  ...config,
                  concurrency: Number(event.target.value) || 1,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              {capped
                ? `账号只够开 ${status?.workers} 个会话，多出来的并发不会生效`
                : "一个账号一个会话，上限就是账号池的大小"}
            </span>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            <span className="flex items-center gap-1.5">
              <Timer className="size-3.5" />
              请求间隔（毫秒）
            </span>
            <Input
              type="number"
              min={0}
              max={60000}
              step={100}
              value={config.request_delay_ms}
              onChange={(event) =>
                onChange({
                  ...config,
                  request_delay_ms: Number(event.target.value) || 0,
                })
              }
            />
            <span className="block font-normal text-muted-foreground">
              每个会话两次请求之间的等待。调小提速，也更显眼。
            </span>
          </label>
        </div>

        <div className="flex flex-wrap items-center gap-4 rounded-lg border bg-muted/25 p-3">
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={config.enabled}
              onCheckedChange={(checked) =>
                onChange({ ...config, enabled: checked })
              }
            />
            随后端启动
          </label>
          <span className="text-xs text-muted-foreground">
            关掉只是不随启动跑，下面的「启动」照样能手动开一轮
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
            启动
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

export function RefetchPanel() {
  const [config, setConfig] = useState<RefetchServiceConfig | null>(null);
  const [status, setStatus] = useState<RefetchStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      setConfig(await jsonRequest<RefetchServiceConfig>("/api/refetch/config"));
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "读取补抓配置失败");
    }
  }, []);

  const loadStatus = useCallback(async () => {
    try {
      setStatus(await jsonRequest<RefetchStatus>("/api/refetch/status"));
    } catch {
      // The status is polled every few seconds and the configuration below it
      // still renders, so one failed poll is not worth a banner of its own.
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => {
      void loadConfig();
      void loadStatus();
    }, 0);
    const timer = window.setInterval(() => void loadStatus(), 3_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadConfig, loadStatus]);

  if (!config) {
    return (
      <Card className="border-border/70 shadow-none">
        <CardContent className="flex h-36 flex-col items-center justify-center gap-3 text-sm text-muted-foreground">
          {error ? (
            <>
              <p className="text-center text-xs text-red-700 dark:text-red-300">
                读取补抓配置失败：{error}
              </p>
              <Button variant="outline" size="sm" onClick={() => void loadConfig()}>
                重试
              </Button>
            </>
          ) : (
            "正在读取补抓配置…"
          )}
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <RefetchStatusCard
        status={status}
        // What is running, falling back to the form only before a first run.
        // Otherwise opening the dropdown would relabel a pb repair's counters
        // as a catalogue sweep's without anything having changed.
        work={status?.active_work ?? config.work}
        onRefresh={() => void loadStatus()}
      />
      <RefetchConfigCard
        config={config}
        onChange={setConfig}
        status={status}
        onReload={loadConfig}
      />
    </div>
  );
}
