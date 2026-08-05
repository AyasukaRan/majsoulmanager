"use client";

import { useCallback, useEffect, useState } from "react";
import { KeyRound, Plus, Save, Trash2 } from "lucide-react";

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
  const { busy, message, run } = useBusyAction();

  const load = useCallback(async () => {
    try {
      setDocument(await jsonRequest<AccountDocument>("/api/accounts"));
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
            {accounts.map((account, index) => (
              <div
                key={index}
                className="grid gap-2 rounded-lg border bg-muted/25 p-3 md:grid-cols-[1.4fr_1.1fr_140px_1fr_auto] md:items-center"
              >
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
                    onClick={() =>
                      setDocument({
                        ...document_,
                        accounts: accounts.filter((_, at) => at !== index),
                      })
                    }
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

        <div className="flex flex-wrap gap-2">
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
