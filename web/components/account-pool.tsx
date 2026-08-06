"use client";

import { useCallback, useEffect, useState } from "react";
import { KeyRound, Plus, Save, Trash2 } from "lucide-react";

import { parseAccountsJsonl } from "@/lib/accounts-jsonl.mjs";
import {
  ApiRequestError,
  jsonRequest,
  type AccountDocument,
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
  { value: "watch", label: "实时采集" },
  { value: "refetch", label: "批量补抓" },
];

/**
 * The accounts this deployment logs in with.
 *
 * A password is never sent back: a stored one reads as `***`, and handing that
 * straight back on save keeps it — so editing a note does not wipe it. What is
 * typed into the box does replace it.
 *
 * The 用途 column is not a label. Mahjong Soul allows one session per account, so
 * an account both halves might use is one they will kick each other off, and
 * what is lost while a collector is locked out is games that were being played
 * at the time — nothing fetches those a second time. So the split is enforced:
 * the same account cannot appear twice, and the re-fetch pool treats every
 * 实时采集 row as spoken for even when it is switched off.
 */
export function AccountPoolCard() {
  const [document_, setDocument] = useState<AccountDocument | null>(null);
  const [error, setError] = useState<string | null>(null);
  /**
   * Which rows the batch controls act on, by position.
   *
   * By position rather than by id because a row imported or added but not yet
   * saved has no id — the backend assigns it — and those are exactly the rows a
   * batch edit is for. The cost is that a delete shifts every later row up, so
   * a delete clears it rather than leave a tick sitting on whatever moved into
   * that slot; a reload clears it too, since the list came from elsewhere.
   * Adding and importing only append, so what is ticked stays ticked.
   */
  const [selected, setSelected] = useState<ReadonlySet<number>>(new Set());
  const { busy, message, run } = useBusyAction();

  const load = useCallback(async () => {
    try {
      setDocument(await jsonRequest<AccountDocument>("/api/accounts"));
      setSelected(new Set());
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "读取账号池失败");
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void load(), 0);
    return () => window.clearTimeout(initial);
  }, [load]);

  async function save() {
    if (!document_) return;
    await run("保存失败", async () => {
      try {
        const saved = await jsonRequest<AccountDocument>("/api/accounts", {
          method: "PUT",
          body: JSON.stringify(document_),
        });
        setDocument(saved);
        return `账号池 r${saved.revision} 已保存`;
      } catch (failure) {
        if (failure instanceof ApiRequestError && failure.status === 412) {
          await load();
          throw new Error("账号池已被其他人修改，已重新载入，请重新编辑后保存");
        }
        throw failure;
      }
    });
  }

  if (!document_) {
    return (
      <Card className="border-border/70 shadow-none">
        <CardContent className="flex h-28 items-center justify-center text-sm text-muted-foreground">
          {error ?? "正在读取账号池…"}
        </CardContent>
      </Card>
    );
  }

  const accounts = document_.accounts;
  const edit = (index: number, patch: Partial<StoredAccount>) =>
    setDocument({
      ...document_,
      accounts: accounts.map((account, at) =>
        at === index ? { ...account, ...patch } : account,
      ),
    });
  /** The same edit, applied to every ticked row at once. */
  const editSelected = (patch: Partial<StoredAccount>) =>
    setDocument({
      ...document_,
      accounts: accounts.map((account, at) =>
        selected.has(at) ? { ...account, ...patch } : account,
      ),
    });
  const removeAt = (drop: (index: number) => boolean) => {
    setDocument({
      ...document_,
      accounts: accounts.filter((_, at) => !drop(at)),
    });
    setSelected(new Set());
  };
  const toggle = (index: number, on: boolean) =>
    setSelected((previous) => {
      const next = new Set(previous);
      if (on) next.add(index);
      else next.delete(index);
      return next;
    });
  /**
   * Stages the registrar's output, it does not save it: what lands is rows in
   * the list above, for an operator to look at before pressing 保存. Fifty
   * accounts arriving in one PUT that then fails validation would otherwise be
   * fifty rows to find the bad one in.
   */
  async function importJsonl(file: File) {
    if (!document_) return;
    await run("导入失败", async () => {
      const { accounts: added, duplicates, unusable } = parseAccountsJsonl(
        await file.text(),
        accounts.map((account) => account.username),
      );
      if (added.length > 0) {
        // Appended to whatever is on screen when the file finishes reading, not
        // to the snapshot this handler started with: a row deleted or a note
        // typed while the file was being read would otherwise come back.
        setDocument((previous) =>
          previous
            ? {
                ...previous,
                accounts: [
                  ...previous.accounts,
                  ...added.map((account) => ({
                    // Empty: the backend assigns one on save and hands it back.
                    id: "",
                    ...account,
                    purpose: "refetch" as const,
                    enabled: true,
                  })),
                ],
              }
            : previous,
        );
      }
      // Named for what was actually skipped. A repeat inside the file is not
      // "already in the pool", and an operator told that goes looking through
      // the list for a row that was never there.
      const skipped = [
        duplicates > 0 ? `${duplicates} 个重复的（池里已有，或文件里重了）` : null,
        unusable > 0
          ? `${unusable} 行用不了（读不出、没密码，或账号里带空格和 /）`
          : null,
      ]
        .filter(Boolean)
        .join("，");
      if (added.length === 0 && !skipped) {
        throw new Error("这个文件里一个账号都没有");
      }
      return [
        `${added.length} 个账号已加进列表，用途是批量补抓`,
        skipped ? `；跳过 ${skipped}` : "",
        // Only when something is waiting to be saved: after an import that
        // added nothing, "点保存才写进去" reads as an instruction to save the
        // rows that were just thrown away.
        added.length > 0 ? "。还没保存，点保存才写进去。" : "。",
      ].join("");
    });
  }

  const counts = PURPOSES.map(({ value, label }) => ({
    label,
    value: accounts.filter(
      (account) => account.enabled && account.purpose === value,
    ).length,
  }));

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="border-b">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <CardTitle className="flex flex-wrap items-center gap-2">
              <KeyRound className="size-4" />
              账号池
              {counts.map(({ label, value }) => (
                <Badge key={label} variant="outline" className="font-mono">
                  {label} {value}
                </Badge>
              ))}
            </CardTitle>
            <CardDescription className="mt-1">
              {error ??
                "密码存在管理 API 的数据目录里（0600），读出来永远是 ***；原样存回不会覆盖，要换就直接输入新的。"}
            </CardDescription>
          </div>
          <Badge variant="outline" className="font-mono">
            r{document_.revision}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-5">
        {accounts.length === 0 ? (
          <p className="rounded-lg border border-dashed px-3 py-6 text-center text-sm text-muted-foreground">
            还没有账号。加一个，然后到 Watch 设置或牌谱补抓页把它选上。
          </p>
        ) : (
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-3 px-1 text-xs text-muted-foreground">
              <label className="flex items-center gap-1.5">
                <Checkbox
                  aria-label="全选"
                  checked={selected.size === accounts.length}
                  onCheckedChange={(checked) =>
                    setSelected(
                      checked ? new Set(accounts.map((_, at) => at)) : new Set(),
                    )
                  }
                />
                全选
              </label>
              {selected.size > 0 ? (
                <>
                  <span className="font-mono">已选 {selected.size} 个</span>
                  {PURPOSES.map(({ value, label }) => (
                    <Button
                      key={value}
                      variant="outline"
                      size="sm"
                      onClick={() => editSelected({ purpose: value })}
                    >
                      设为{label}
                    </Button>
                  ))}
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => editSelected({ enabled: true })}
                  >
                    启用
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => editSelected({ enabled: false })}
                  >
                    停用
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => removeAt((at) => selected.has(at))}
                  >
                    <Trash2 className="size-4" />
                    删除
                  </Button>
                </>
              ) : (
                <span>
                  {"勾上几行就能一次改用途、一次启停、一次删除。改完照样要点保存。"}
                </span>
              )}
            </div>
            {accounts.map((account, index) => (
              <div
                key={index}
                className="grid gap-2 rounded-lg border bg-muted/25 p-3 md:grid-cols-[auto_1.4fr_1.1fr_140px_1fr_auto] md:items-center"
              >
                <Checkbox
                  aria-label={`选中 ${account.username || "这一行"}`}
                  checked={selected.has(index)}
                  onCheckedChange={(checked) => toggle(index, checked === true)}
                />
                <label className="space-y-1 text-xs font-medium md:space-y-0">
                  <span className="md:hidden">账号</span>
                  <Input
                    value={account.username}
                    placeholder="账号"
                    onChange={(event) =>
                      edit(index, { username: event.target.value })
                    }
                  />
                </label>
                <label className="space-y-1 text-xs font-medium md:space-y-0">
                  <span className="md:hidden">密码</span>
                  {/* A real secret field, like every other one in this
                      console. It holds `***` for a stored account, so nothing
                      is hidden that was not already hidden — but a plain input
                      invites a browser to offer it to a password manager and
                      shows it to whoever is behind the operator. */}
                  <Input
                    type="password"
                    autoComplete="new-password"
                    value={account.password}
                    placeholder="密码"
                    onChange={(event) =>
                      edit(index, { password: event.target.value })
                    }
                  />
                </label>
                <select
                  className={selectClass}
                  value={account.purpose}
                  onChange={(event) =>
                    edit(index, {
                      purpose: event.target.value as StoredAccount["purpose"],
                    })
                  }
                >
                  {PURPOSES.map(({ value, label }) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
                <Input
                  value={account.note}
                  placeholder="备注（可空）"
                  onChange={(event) => edit(index, { note: event.target.value })}
                />
                <div className="flex items-center gap-3">
                  <label className="flex items-center gap-1.5 text-xs">
                    <Checkbox
                      checked={account.enabled}
                      onCheckedChange={(checked) =>
                        edit(index, { enabled: checked })
                      }
                    />
                    启用
                  </label>
                  <Button
                    variant="ghost"
                    size="sm"
                    aria-label="删除这个账号"
                    onClick={() => removeAt((at) => at === index)}
                  >
                    <Trash2 className="size-4" />
                  </Button>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* One string rather than three lines of JSX: a line break between two
            Chinese characters renders as a space, which reads as a typo. */}
        <p className="text-xs leading-relaxed text-muted-foreground">
          停用只是不拿它登录，<strong>不会</strong>
          {"把它让给另一边——「实时采集」下的账号始终算采集占着的，补抓池永远不碰。雀魂一个账号只能开一个会话，两边抢会把实时采集踢下线，而那段时间正在打的对局没有第二次机会。"}
        </p>

        <p className="text-xs leading-relaxed text-muted-foreground">
          {"注册脚本产出的 accounts.jsonl（每行一个 email / password / nickname）可以直接选进来，一次加一批「批量补抓」的账号；重复的和用不了的会跳过，加完还得自己点保存。"}
        </p>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            onClick={() =>
              setDocument({
                ...document_,
                accounts: [
                  ...accounts,
                  {
                    // Empty: the backend assigns one on save and hands it back.
                    id: "",
                    username: "",
                    password: "",
                    purpose: "refetch",
                    note: "",
                    enabled: true,
                  },
                ],
              })
            }
          >
            <Plus className="size-4" />
            加一个
          </Button>
          <Input
            type="file"
            accept=".jsonl,.json,application/json,text/plain"
            aria-label="导入注册脚本产出的 accounts.jsonl"
            // A file input is as wide as its own button and filename together,
            // which is most of a phone screen: it takes a row of its own there
            // rather than squeezing 加一个 and 保存 out of shape.
            className="w-full min-w-0 sm:w-auto"
            disabled={busy}
            onChange={(event) => {
              const file = event.target.files?.[0];
              // Cleared first, so picking the same file again is another change
              // event — an import that was rejected has to be retryable after
              // the file is fixed.
              event.target.value = "";
              if (file) void importJsonl(file);
            }}
          />
          <Button onClick={() => void save()} disabled={busy}>
            <Save className="size-4" />
            保存
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
