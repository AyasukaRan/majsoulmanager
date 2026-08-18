"use client";

import { useCallback, useEffect, useState } from "react";
import { UserPlus } from "lucide-react";

import {
  jsonRequest,
  type AccountRegisterProgress,
  type StoredAccount,
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

const PURPOSES: Array<{ value: StoredAccount["purpose"]; label: string }> = [
  { value: "refetch", label: "批量补抓" },
  { value: "watch", label: "实时采集" },
];

/**
 * How often the progress is polled while a run is going.
 *
 * One account takes minutes, so this is not about smoothness — it is so that a
 * page opened halfway through a run finds out there is one within a few seconds,
 * and so that 正在注册 X changes when it moves on.
 */
const POLL_MS = 5_000;

function countMailboxes(text: string): number {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#")).length;
}

/**
 * Registering Mahjong Soul accounts from the console.
 *
 * The work is done by an installed `register` module — there is no builtin,
 * because rustls cannot produce Chrome's ClientHello and a brand new account has
 * nothing else to be judged on. Without one, 开始注册 answers with what to
 * install rather than doing it badly.
 *
 * Each account that succeeds lands in the pool **disabled**, one at a time, as
 * it finishes. That is deliberate and it is why this is worth a background run
 * rather than a request: an account that was created and not stored is gone —
 * its password only ever existed inside the run that made it — so closing this
 * page must not be able to lose the ones already made.
 */
export function AccountRegisterCard() {
  const [mailboxes, setMailboxes] = useState("");
  const [purpose, setPurpose] = useState<StoredAccount["purpose"]>("refetch");
  const [note, setNote] = useState("");
  const [proxy, setProxy] = useState("");
  const [mimic, setMimic] = useState(true);
  const [progress, setProgress] = useState<AccountRegisterProgress | null>(null);
  const { busy, message, run } = useBusyAction();

  const poll = useCallback(async () => {
    try {
      setProgress(
        await jsonRequest<AccountRegisterProgress>(
          "/api/accounts/register/status",
        ),
      );
    } catch {
      // Never fatal: the form still works, and an error banner over it would
      // suggest the run itself had failed when only the poll did.
    }
  }, []);

  // Off the render pass, the way the pool card loads its own document: a poll
  // that resolves synchronously would set state inside the effect body.
  useEffect(() => {
    const initial = window.setTimeout(() => void poll(), 0);
    return () => window.clearTimeout(initial);
  }, [poll]);

  // Only while something is running. A finished run's numbers do not change, and
  // a timer left going would keep the page awake for nothing.
  useEffect(() => {
    if (!progress?.running) return;
    const timer = window.setInterval(() => void poll(), POLL_MS);
    return () => window.clearInterval(timer);
  }, [progress?.running, poll]);

  const pending = countMailboxes(mailboxes);
  const running = progress?.running ?? false;

  async function start() {
    await run("注册启动失败", async () => {
      const started = await jsonRequest<AccountRegisterProgress>(
        "/api/accounts/register",
        {
          method: "POST",
          body: JSON.stringify({
            mailboxes: mailboxes.split("\n"),
            purpose,
            note,
            proxy: proxy.trim() || null,
            mimic,
          }),
        },
      );
      setProgress(started);
      // Cleared on success only: a batch that was refused for a bad line has to
      // still be on screen to be fixed.
      setMailboxes("");
      return `开始注册 ${started.total} 个账号，进度在下面`;
    });
  }

  async function stop() {
    await run("停止失败", async () => {
      await jsonRequest("/api/accounts/register/stop", { method: "POST" });
      await poll();
      return "已请求停止，当前这个账号注册完就结束";
    });
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <UserPlus className="size-4" />
          注册新账号
        </CardTitle>
        <CardDescription>
          用装好的 register 模块直接注册，成功的号会立刻以「停用」状态写进上面的账号池，
          确认后自己启用。拟真开着时一个号 3~5 分钟，中途关掉页面不影响已经建好的。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <label className="block space-y-1.5 text-xs font-medium">
          邮箱凭据（一行一个，行首 # 跳过）
          <textarea
            className="block h-28 w-full rounded-md border bg-transparent px-3 py-2 font-mono text-xs"
            placeholder={"abcd1234@outlook.com----密码----clientId----refreshToken"}
            value={mailboxes}
            disabled={running}
            onChange={(event) => setMailboxes(event.target.value)}
          />
          <span className="block font-normal text-muted-foreground">
            凭据串里含邮箱地址，注册用地址、取码用整串。它不会出现在日志和进度里 ——
            那两处只有邮箱地址。
          </span>
        </label>

        <div className="grid gap-3 sm:grid-cols-3">
          <label className="space-y-1.5 text-xs font-medium">
            用途
            <select
              className={selectClass}
              value={purpose}
              disabled={running}
              onChange={(event) =>
                setPurpose(event.target.value as StoredAccount["purpose"])
              }
            >
              {PURPOSES.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            备注
            <Input
              value={note}
              disabled={running}
              placeholder="这批号是什么时候、哪来的"
              onChange={(event) => setNote(event.target.value)}
            />
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            出口代理（可选）
            <Input
              value={proxy}
              disabled={running}
              placeholder="http://127.0.0.1:7890"
              onChange={(event) => setProxy(event.target.value)}
            />
          </label>
        </div>

        <label className="flex items-start gap-2 text-xs">
          <Checkbox
            checked={mimic}
            disabled={running}
            onCheckedChange={(value) => setMimic(value === true)}
          />
          <span>
            拟真会话
            <span className="block font-normal text-muted-foreground">
              复刻真实客户端的心跳、大厅拉取和停顿。关掉每个号快四分钟左右，但那样的连接
              全程 0 条心跳、2 秒即断，服务端一条「收到 loginSuccess 却从没收到 fetchInfo」
              就能认出来。只在做对照实验时关。
            </span>
          </span>
        </label>

        <div className="flex flex-wrap items-center gap-2">
          <Button onClick={() => void start()} disabled={busy || running || pending === 0}>
            开始注册{pending > 0 ? ` ${pending} 个` : ""}
          </Button>
          {running ? (
            <Button variant="outline" onClick={() => void stop()} disabled={busy}>
              停止
            </Button>
          ) : null}
          {message ? (
            <span className="text-xs text-muted-foreground">{message}</span>
          ) : null}
        </div>

        {progress && (progress.total > 0 || progress.message) ? (
          <div className="space-y-2 rounded-md border p-3">
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <Badge variant={running ? "default" : "secondary"}>
                {running ? "注册中" : "已结束"}
              </Badge>
              <span>
                {progress.done}/{progress.total}
              </span>
              <span className="text-emerald-600 dark:text-emerald-400">
                成功 {progress.succeeded}
              </span>
              <span className={progress.failed > 0 ? "text-destructive" : ""}>
                失败 {progress.failed}
              </span>
              {progress.current ? (
                <span className="text-muted-foreground">
                  正在注册 {progress.current}
                </span>
              ) : null}
            </div>
            {/* Deliberately not reloading the pool from here: the list above
                is an editor, and pulling a fresh document into it would throw
                away whatever is typed but unsaved. */}
            {progress.succeeded > 0 ? (
              <p className="text-xs text-muted-foreground">
                {progress.succeeded} 个已经写进账号池了（停用状态）。刷新本页在上面的列表里看到它们。
              </p>
            ) : null}
            {progress.message ? (
              <p className="text-xs text-muted-foreground">{progress.message}</p>
            ) : null}
            {progress.outcomes.length > 0 ? (
              // Newest first: a long run's interesting end is the part an
              // operator came back to look at.
              <ul className="max-h-56 space-y-1 overflow-y-auto text-xs">
                {[...progress.outcomes].reverse().map((outcome) => (
                  <li key={`${outcome.email}-${outcome.at}`} className="flex gap-2">
                    <span
                      className={
                        outcome.ok
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-destructive"
                      }
                    >
                      {outcome.ok ? "✓" : "✗"}
                    </span>
                    <span className="font-mono">{outcome.email}</span>
                    <span className="text-muted-foreground">
                      {outcome.ok
                        ? outcome.detail
                        : `[${outcome.stage}] ${outcome.detail}`}
                    </span>
                  </li>
                ))}
              </ul>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
