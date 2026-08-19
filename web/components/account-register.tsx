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
 * Where the addresses come from.
 *
 * `list` is prepared third-party mailboxes — a consumable, and the last batch
 * ran out. The other two open an address per account at registration time:
 * `cloud` on a Cloud Mail instance you administer, `temp` on a temp-mail service
 * that needs nothing but a key. Nothing to buy either way, and no naming pattern
 * carried over between batches.
 */
type MailSource = "list" | "cloud" | "temp";

/**
 * Where the non-secret half of the Cloud Mail settings is remembered.
 *
 * The password and the token are deliberately not in here. Either one is the
 * key to every mailbox on that instance, and localStorage is readable by
 * anything that gets a script onto this page. The instance address and the
 * administrator's address are not secrets, and they are the tedious part to
 * retype.
 */
const CLOUD_MAIL_KEY = "mjai.cloud-mail";

/** Same rule for the temp-mail service: the address, never the key. */
const TEMP_MAIL_KEY = "mjai.temp-mail";

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
 * Mailboxes come from one of two places, see {@link MailSource}. Cloud Mail is
 * the one that scales: the module opens an address per account, so a run is a
 * number rather than a list somebody had to source first.
 *
 * Each account that succeeds lands in the pool **disabled**, one at a time, as
 * it finishes. That is deliberate and it is why this is worth a background run
 * rather than a request: an account that was created and not stored is gone —
 * its password only ever existed inside the run that made it — so closing this
 * page must not be able to lose the ones already made.
 */
export function AccountRegisterCard() {
  const [source, setSource] = useState<MailSource>("list");
  const [mailboxes, setMailboxes] = useState("");
  const [cloudUrl, setCloudUrl] = useState("");
  const [cloudAdmin, setCloudAdmin] = useState("");
  const [cloudPassword, setCloudPassword] = useState("");
  const [tempUrl, setTempUrl] = useState("");
  const [tempKey, setTempKey] = useState("");
  const [count, setCount] = useState(5);
  const [concurrency, setConcurrency] = useState(1);
  const [randomNode, setRandomNode] = useState(false);
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

  // Same reason for the timeout, plus localStorage does not exist while this
  // renders on the server.
  useEffect(() => {
    const restore = window.setTimeout(() => {
      try {
        const cloud = JSON.parse(
          window.localStorage.getItem(CLOUD_MAIL_KEY) ?? "{}",
        ) as { base_url?: string; admin_email?: string };
        setCloudUrl(cloud.base_url ?? "");
        setCloudAdmin(cloud.admin_email ?? "");
        const temp = JSON.parse(
          window.localStorage.getItem(TEMP_MAIL_KEY) ?? "{}",
        ) as { base_url?: string };
        setTempUrl(temp.base_url ?? "");
      } catch {
        // Somebody else's key, or a half-written one. Typing them again is a
        // smaller problem than a card that refuses to render.
      }
    }, 0);
    return () => window.clearTimeout(restore);
  }, []);

  // Only while something is running. A finished run's numbers do not change, and
  // a timer left going would keep the page awake for nothing.
  useEffect(() => {
    if (!progress?.running) return;
    const timer = window.setInterval(() => void poll(), POLL_MS);
    return () => window.clearInterval(timer);
  }, [progress?.running, poll]);

  const ready =
    source === "cloud"
      ? cloudUrl.trim().length > 0 &&
        cloudAdmin.trim().length > 0 &&
        cloudPassword.length > 0
      : source === "temp"
        ? tempKey.trim().length > 0
        : true;
  const pending =
    source === "list"
      ? countMailboxes(mailboxes)
      : ready
        ? count
        : 0;
  const running = progress?.running ?? false;

  async function start() {
    await run("注册启动失败", async () => {
      const common = {
        purpose,
        note,
        proxy: proxy.trim() || null,
        mimic,
        concurrency,
        random_node: randomNode,
      };
      // No domain: the module asks the instance which ones it receives and
      // spreads the batch over them.
      const cloud = {
        base_url: cloudUrl.trim(),
        admin_email: cloudAdmin.trim(),
        admin_password: cloudPassword,
      };
      // Empty base_url means the module's own default, so a key is enough.
      const temp = { base_url: tempUrl.trim(), api_key: tempKey.trim() };
      const body =
        source === "cloud"
          ? { ...common, cloud_mail: cloud, count }
          : source === "temp"
            ? { ...common, temp_mail: temp, count }
            : { ...common, mailboxes: mailboxes.split("\n") };
      const started = await jsonRequest<AccountRegisterProgress>(
        "/api/accounts/register",
        { method: "POST", body: JSON.stringify(body) },
      );
      setProgress(started);
      // Remembered on success only, so a rejected address is not the one that
      // comes back next time. The password is never stored — see CLOUD_MAIL_KEY.
      if (source === "cloud") {
        window.localStorage.setItem(
          CLOUD_MAIL_KEY,
          JSON.stringify({
            base_url: cloud.base_url,
            admin_email: cloud.admin_email,
          }),
        );
      } else if (source === "temp") {
        // Only the address. The key reads every mailbox the service hands out.
        window.localStorage.setItem(
          TEMP_MAIL_KEY,
          JSON.stringify({ base_url: temp.base_url }),
        );
      } else {
        // Cleared on success only: a batch that was refused for a bad line has
        // to still be on screen to be fixed.
        setMailboxes("");
      }
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
          邮箱可以自己备，也可以一个号现开一个 —— 给个临时邮箱 API 的 key 最省事。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <label className="block space-y-1.5 text-xs font-medium sm:w-64">
          邮箱来源
          <select
            className={selectClass}
            value={source}
            disabled={running}
            onChange={(event) => setSource(event.target.value as MailSource)}
          >
            <option value="list">凭据列表（自己备好的邮箱）</option>
            <option value="temp">临时邮箱 API（现开，最省事）</option>
            <option value="cloud">Cloud Mail（现开，自己的实例）</option>
          </select>
        </label>

        {source === "list" ? (
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
        ) : source === "temp" ? (
          <div className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-4">
              <label className="space-y-1.5 text-xs font-medium sm:col-span-2">
                API key
                <Input
                  type="password"
                  value={tempKey}
                  disabled={running}
                  placeholder="sk-..."
                  onChange={(event) => setTempKey(event.target.value)}
                />
              </label>
              <label className="space-y-1.5 text-xs font-medium">
                注册数量
                <Input
                  type="number"
                  min={1}
                  max={200}
                  value={count}
                  disabled={running}
                  onChange={(event) =>
                    setCount(Math.max(0, Number(event.target.value) || 0))
                  }
                />
              </label>
              <label className="space-y-1.5 text-xs font-medium">
                接口地址（可选）
                <Input
                  value={tempUrl}
                  disabled={running}
                  placeholder="留空用默认"
                  onChange={(event) => setTempUrl(event.target.value)}
                />
              </label>
            </div>
            <p className="text-xs text-muted-foreground">
              最省事的一条：一个 key 就够，不用建邮箱也不用填域名。地址由服务端给，
              每个号换一个域名，本地名也像真人 —— 这两件事自己做都做不好。
            </p>
            <p className="text-xs text-muted-foreground">
              代价是这些邮箱在服务方眼皮底下：验证码信他们看得见，也就意味着这批号的
              找回邮箱不是你独占的。采集号无所谓，别拿它注册你在乎的东西。key 不记在
              本机，接口地址记。
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-4">
              <label className="space-y-1.5 text-xs font-medium sm:col-span-2">
                实例地址
                <Input
                  value={cloudUrl}
                  disabled={running}
                  placeholder="https://mail.example.com"
                  onChange={(event) => setCloudUrl(event.target.value)}
                />
              </label>
              <label className="space-y-1.5 text-xs font-medium">
                管理员邮箱
                <Input
                  value={cloudAdmin}
                  disabled={running}
                  placeholder="admin@example.com"
                  onChange={(event) => setCloudAdmin(event.target.value)}
                />
              </label>
              <label className="space-y-1.5 text-xs font-medium">
                管理员密码
                <Input
                  type="password"
                  value={cloudPassword}
                  disabled={running}
                  onChange={(event) => setCloudPassword(event.target.value)}
                />
              </label>
            </div>
            <label className="block space-y-1.5 text-xs font-medium sm:w-40">
              注册数量
              <Input
                type="number"
                min={1}
                max={200}
                value={count}
                disabled={running}
                onChange={(event) =>
                  setCount(Math.max(0, Number(event.target.value) || 0))
                }
              />
            </label>
            <p className="text-xs text-muted-foreground">
              域名不用填 —— 开跑前先问一次实例在收哪几个，然后把这批号随机分到上面去。
              地址和管理员邮箱记在本机，密码不记：拿着它能读这个实例上所有人的邮件。
            </p>
            <p className="text-xs text-muted-foreground">
              得是你有管理员账号的实例。公共的临时邮箱站不行 —— 那边注册要过人机验证，
              而且多半关了「一个账号多个邮箱」，一个地址只能注册一个雀魂号。
            </p>
          </div>
        )}

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

        <div className="grid gap-3 sm:grid-cols-3">
          <label className="space-y-1.5 text-xs font-medium">
            同时注册几个
            <Input
              type="number"
              min={1}
              max={16}
              value={concurrency}
              disabled={running}
              onChange={(event) =>
                setConcurrency(
                  Math.min(16, Math.max(1, Number(event.target.value) || 1)),
                )
              }
            />
            <span className="block font-normal text-muted-foreground">
              一个号的时间九成是拟真停顿，压不下去，只能并着跑。
              {concurrency > 1
                ? ` ${count} 个约 ${Math.ceil((count / concurrency) * 3)} 分钟。`
                : " 一个一个来的话，一个号 3~5 分钟。"}
            </span>
          </label>
        </div>

        <label className="flex items-start gap-2 text-xs">
          <Checkbox
            checked={randomNode}
            disabled={running}
            onCheckedChange={(value) => setRandomNode(value === true)}
          />
          <span>
            每个号换一个出口节点
            <span className="block font-normal text-muted-foreground">
              轮着用 mihomo 上已经建好出站的节点，注册成功后把节点记进账号 ——
              以后补抓登录它，走的是它注册时那个出口。可用的节点跟着账号池里
              「已启用的补抓账号绑了哪些节点」走；一个都没有的话这里会直接拒绝开跑，
              而不是偷偷全从一个地址出去。
            </span>
          </span>
        </label>

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
