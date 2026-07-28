"use client";

import { useMemo } from "react";
import {
  CirclePlay,
  CircleStop,
  RotateCw,
  Save,
  Settings2,
} from "lucide-react";

import {
  jsonRequest,
  type InstalledWatchModule,
  type WatchModuleRef,
  type WatchServiceConfig,
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
import type { RunAction } from "@/components/watch/busy-action";
import { WatchInstanceList } from "@/components/watch/instance-list";
import { WatchModuleInstaller } from "@/components/watch/module-installer";

function moduleValue(module: WatchModuleRef) {
  return `${module.name}@${module.version}`;
}

function parseModule(value: string): WatchModuleRef {
  const split = value.lastIndexOf("@");
  return { name: value.slice(0, split), version: value.slice(split + 1) };
}

export function WatchServiceCard({
  config,
  onChange,
  modules,
  busy,
  run,
  onMessage,
  onReload,
}: {
  config: WatchServiceConfig;
  onChange: (next: WatchServiceConfig) => void;
  modules: InstalledWatchModule[];
  busy: boolean;
  run: RunAction;
  onMessage: (message: string) => void;
  onReload: () => Promise<void>;
}) {
  const loginModules = useMemo(
    () => modules.filter((module) => module.kind === "login"),
    [modules],
  );
  const pbModules = useMemo(
    () => modules.filter((module) => module.kind === "pb_fetch"),
    [modules],
  );

  async function saveConfig() {
    await run("保存失败", async () => {
      const saved = await jsonRequest<WatchServiceConfig>("/api/watch/config", {
        method: "PUT",
        body: JSON.stringify(config),
      });
      // The response carries the key the backend assigned to every instance
      // created here, and the next save hands it straight back.
      onChange(saved);
      return `配置 r${saved.revision} 已保存并应用`;
    });
  }

  async function serviceAction(action: "start" | "stop" | "reload") {
    await run("操作失败", async () => {
      await jsonRequest("/api/watch/actions", {
        method: "POST",
        body: JSON.stringify({ action }),
      });
      return action === "start"
        ? "Watch 正在启动"
        : action === "stop"
          ? "Watch 已停止"
          : "新配置与模块已切换";
    });
  }

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="border-b">
        <div className="flex items-start justify-between gap-4">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Settings2 className="size-4" />
              Watch 后台服务
            </CardTitle>
            <CardDescription className="mt-1">
              保存后在线重载，不重启管理 API
            </CardDescription>
          </div>
          <Badge variant="outline" className="font-mono">
            r{config.revision}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-5">
        <label className="block max-w-[220px] space-y-1.5 text-xs font-medium">
          服务器
          <select
            className={selectClass}
            value={config.server}
            onChange={(event) =>
              onChange({ ...config, server: event.target.value as "cn" })
            }
          >
            <option value="cn">国服</option>
            <option value="en">国际服</option>
            <option value="jp">日服</option>
          </select>
        </label>

        <div className="grid gap-3 sm:grid-cols-2">
          <label className="space-y-1.5 text-xs font-medium">
            登录模块
            <select
              className={selectClass}
              value={moduleValue(config.login_module)}
              onChange={(event) =>
                onChange({
                  ...config,
                  login_module: parseModule(event.target.value),
                })
              }
            >
              {loginModules.map((module) => (
                <option
                  key={`${module.name}@${module.version}`}
                  value={`${module.name}@${module.version}`}
                >
                  {module.name} · {module.version}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            PB 获取模块
            <select
              className={selectClass}
              value={moduleValue(config.pb_fetch_module)}
              onChange={(event) =>
                onChange({
                  ...config,
                  pb_fetch_module: parseModule(event.target.value),
                })
              }
            >
              {pbModules.map((module) => (
                <option
                  key={`${module.name}@${module.version}`}
                  value={`${module.name}@${module.version}`}
                >
                  {module.name} · {module.version}
                </option>
              ))}
            </select>
          </label>
        </div>

        <WatchModuleInstaller
          busy={busy}
          run={run}
          onMessage={onMessage}
          onInstalled={onReload}
        />

        <WatchInstanceList
          instances={config.instances}
          onChange={(instances) => onChange({ ...config, instances })}
        />

        <div className="grid gap-3 sm:grid-cols-[180px_1fr]">
          <label className="space-y-1.5 text-xs font-medium">
            Watch 出站
            <select
              className={selectClass}
              value={config.proxy_mode}
              onChange={(event) =>
                onChange({
                  ...config,
                  proxy_mode: event.target.value as
                    | "direct"
                    | "mihomo"
                    | "custom",
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
            总开关；实例还要各自启用才会连接
          </span>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void saveConfig()} disabled={busy}>
            <Save className="size-4" />
            保存并应用
          </Button>
          <Button
            variant="outline"
            onClick={() => void serviceAction("start")}
            disabled={busy}
          >
            <CirclePlay className="size-4" />
            启动
          </Button>
          <Button
            variant="outline"
            onClick={() => void serviceAction("reload")}
            disabled={busy}
          >
            <RotateCw className="size-4" />
            热重载
          </Button>
          <Button
            variant="outline"
            onClick={() => void serviceAction("stop")}
            disabled={busy}
          >
            <CircleStop className="size-4" />
            停止
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
