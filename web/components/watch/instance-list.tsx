"use client";

import { Plus, Trash2 } from "lucide-react";

import type { WatchInstance } from "@/lib/mjai-api";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { selectClass } from "@/lib/utils";

/** A new instance starts disabled with an id the backend will accept as unique. */
function newInstance(existing: WatchInstance[]): WatchInstance {
  const taken = new Set(existing.map((instance) => instance.id));
  let id = "collector";
  for (let suffix = existing.length + 1; taken.has(id); suffix += 1) {
    id = `collector-${suffix}`;
  }
  return {
    id,
    enabled: false,
    // Matches the backend's own default, and for its reason: a room that was
    // never watched is games nobody can fetch afterwards, while a room that was
    // watched and is not wanted is one filter away.
    room: "all",
    players: 4,
    modes: ["east", "south"],
    // Left blank on purpose: every collector needs its own account, and the
    // backend rejects two instances sharing one secret ref.
    account_secret_ref: "",
    client_version: null,
  };
}

export function WatchInstanceList({
  instances,
  onChange,
}: {
  instances: WatchInstance[];
  onChange: (next: WatchInstance[]) => void;
}) {
  // Patching rather than replacing, because an instance carries a key this
  // console never renders and must hand back untouched on the next save.
  const updateInstance = (index: number, patch: Partial<WatchInstance>) =>
    onChange(
      instances.map((instance, other) =>
        other === index ? { ...instance, ...patch } : instance,
      ),
    );

  return (
    <div className="space-y-3 rounded-lg border bg-muted/25 p-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <p className="text-xs font-medium">采集实例</p>
          <p className="mt-1 text-xs text-muted-foreground">
            每个实例用一个账号盯一种房间和人数，可以同时跑三麻和四麻。
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => onChange([...instances, newInstance(instances)])}
        >
          <Plus className="size-4" />
          新增实例
        </Button>
      </div>

      {instances.map((instance, index) => (
        <div
          // Index is a fine key here: every field is controlled, so a
          // delete re-renders the shifted rows from state anyway.
          key={index}
          className="space-y-3 rounded-lg border bg-background p-3"
        >
          <div className="grid gap-3 sm:grid-cols-[1fr_auto_auto]">
            <label className="space-y-1.5 text-xs font-medium">
              实例 ID
              <Input
                value={instance.id}
                onChange={(event) =>
                  updateInstance(index, { id: event.target.value })
                }
                placeholder="four-player"
              />
              <span className="block font-normal text-muted-foreground">
                只能用字母、数字和 . _ -，仅作显示和日志标识，可以随时改名
              </span>
            </label>
            <label className="space-y-1.5 text-xs font-medium">
              房间
              <select
                className={selectClass}
                value={instance.room}
                onChange={(event) =>
                  updateInstance(index, {
                    room: event.target.value as "jade",
                  })
                }
              >
                <option value="gold">金之间</option>
                <option value="jade">玉之间</option>
                <option value="throne">王座之间</option>
                <option value="all">全部</option>
              </select>
            </label>
            <label className="space-y-1.5 text-xs font-medium">
              人数
              <select
                className={selectClass}
                value={instance.players}
                onChange={(event) =>
                  updateInstance(index, {
                    players: Number(event.target.value) as 3 | 4,
                  })
                }
              >
                <option value={4}>四麻</option>
                <option value={3}>三麻</option>
              </select>
            </label>
          </div>

          <label className="block space-y-1.5 text-xs font-medium">
            账号密钥引用
            <Input
              value={instance.account_secret_ref}
              onChange={(event) =>
                updateInstance(index, {
                  account_secret_ref: event.target.value,
                })
              }
              placeholder="file:/run/secrets/majsoul_accounts"
            />
            <span className="block font-normal text-muted-foreground">
              仅允许 file: 或 env: 引用，网页不保存明文账号密码
            </span>
          </label>

          <div className="flex flex-wrap items-center gap-4">
            {(["east", "south"] as const).map((mode) => (
              <label key={mode} className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={instance.modes.includes(mode)}
                  onCheckedChange={(checked) =>
                    updateInstance(index, {
                      modes: checked
                        ? [...new Set([...instance.modes, mode])]
                        : instance.modes.filter((value) => value !== mode),
                    })
                  }
                />
                {mode === "east" ? "东风" : "东南"}
              </label>
            ))}
            <label className="flex items-center gap-2 text-sm">
              <Checkbox
                checked={instance.enabled}
                onCheckedChange={(checked) =>
                  updateInstance(index, { enabled: checked === true })
                }
              />
              启用
            </label>
            <Button
              variant="ghost"
              size="sm"
              className="ml-auto text-destructive"
              disabled={instances.length < 2}
              onClick={() =>
                onChange(instances.filter((_, other) => other !== index))
              }
            >
              <Trash2 className="size-4" />
              删除
            </Button>
          </div>
        </div>
      ))}
    </div>
  );
}
