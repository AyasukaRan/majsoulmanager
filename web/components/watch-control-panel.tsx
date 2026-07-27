"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Cable,
  CirclePlay,
  CircleStop,
  Gauge,
  Plus,
  RefreshCw,
  RotateCw,
  Save,
  Settings2,
  Trash2,
} from "lucide-react";

import type {
  InstalledWatchModule,
  MihomoStatus,
  WatchInstance,
  WatchModuleRef,
  WatchServiceConfig,
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

const selectClass =
  "h-9 w-full rounded-lg border border-input bg-background px-3 text-sm outline-none focus:border-ring focus:ring-3 focus:ring-ring/30";

async function jsonRequest<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(path, {
    cache: "no-store",
    ...init,
    headers: {
      "content-type": "application/json",
      ...init?.headers,
    },
  });
  const value = (await response.json()) as T & { error?: string };
  if (!response.ok) {
    throw new Error(value.error ?? `request failed (${response.status})`);
  }
  return value;
}

function moduleValue(module: WatchModuleRef) {
  return `${module.name}@${module.version}`;
}

function parseModule(value: string): WatchModuleRef {
  const split = value.lastIndexOf("@");
  return { name: value.slice(0, split), version: value.slice(split + 1) };
}

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
    room: "jade",
    players: 4,
    modes: ["east", "south"],
    // Left blank on purpose: every collector needs its own account, and the
    // backend rejects two instances sharing one secret ref.
    account_secret_ref: "",
    client_version: null,
  };
}

export function WatchControlPanel() {
  const [config, setConfig] = useState<WatchServiceConfig | null>(null);
  const [modules, setModules] = useState<InstalledWatchModule[]>([]);
  const [proxy, setProxy] = useState<MihomoStatus | null>(null);
  const [subscriptionUrl, setSubscriptionUrl] = useState("");
  const [moduleManifest, setModuleManifest] = useState<File | null>(null);
  const [moduleArtifact, setModuleArtifact] = useState<File | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  // Ids the backend already knows. Renaming one would orphan its state file
  // and drop the games it still has to fetch, so those inputs are locked.
  const [savedIds, setSavedIds] = useState<string[]>([]);

  const updateInstance = useCallback(
    (index: number, patch: Partial<WatchInstance>) =>
      setConfig((current) =>
        current
          ? {
              ...current,
              instances: current.instances.map((instance, other) =>
                other === index ? { ...instance, ...patch } : instance,
              ),
            }
          : current,
      ),
    [],
  );

  const refresh = useCallback(async () => {
    try {
      const [nextConfig, nextModules, nextProxy] = await Promise.all([
        jsonRequest<WatchServiceConfig>("/api/watch/config"),
        jsonRequest<InstalledWatchModule[]>("/api/watch/modules"),
        jsonRequest<MihomoStatus>("/api/watch/proxy"),
      ]);
      setConfig(nextConfig);
      setSavedIds(nextConfig.instances.map((instance) => instance.id));
      setModules(nextModules);
      setProxy(nextProxy);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "读取配置失败");
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    return () => window.clearTimeout(initial);
  }, [refresh]);

  const loginModules = useMemo(
    () => modules.filter((module) => module.kind === "login"),
    [modules],
  );
  const pbModules = useMemo(
    () => modules.filter((module) => module.kind === "pb_fetch"),
    [modules],
  );

  async function saveConfig() {
    if (!config) return;
    setBusy("config");
    setMessage(null);
    try {
      const saved = await jsonRequest<WatchServiceConfig>("/api/watch/config", {
        method: "PUT",
        body: JSON.stringify(config),
      });
      setConfig(saved);
      setSavedIds(saved.instances.map((instance) => instance.id));
      setMessage(`配置 r${saved.revision} 已保存并应用`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "保存失败");
    } finally {
      setBusy(null);
    }
  }

  async function serviceAction(action: "start" | "stop" | "reload") {
    setBusy(action);
    setMessage(null);
    try {
      await jsonRequest("/api/watch/actions", {
        method: "POST",
        body: JSON.stringify({ action }),
      });
      setMessage(
        action === "start"
          ? "Watch 正在启动"
          : action === "stop"
            ? "Watch 已停止"
            : "新配置与模块已切换",
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "操作失败");
    } finally {
      setBusy(null);
    }
  }

  async function updateSubscription() {
    if (!subscriptionUrl) {
      setMessage("请输入订阅链接");
      return;
    }
    setBusy("subscription");
    setMessage(null);
    try {
      const status = await jsonRequest<MihomoStatus>(
        "/api/watch/proxy/subscription",
        {
          method: "PUT",
          body: JSON.stringify({
            url: subscriptionUrl,
            update_interval_secs: proxy?.update_interval_secs ?? 3600,
          }),
        },
      );
      setProxy(status);
      setSubscriptionUrl("");
      setMessage("订阅已写入私密配置并刷新节点");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "订阅更新失败");
    } finally {
      setBusy(null);
    }
  }

  async function installModule() {
    if (!moduleManifest || !moduleArtifact) {
      setMessage("请选择模块 manifest 和可执行文件");
      return;
    }
    setBusy("module");
    setMessage(null);
    try {
      const manifest = JSON.parse(await moduleManifest.text()) as unknown;
      const dataUrl = await new Promise<string>((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result));
        reader.onerror = () => reject(reader.error);
        reader.readAsDataURL(moduleArtifact);
      });
      const artifactBase64 = dataUrl.slice(dataUrl.indexOf(",") + 1);
      const installed = await jsonRequest<InstalledWatchModule>(
        "/api/watch/modules",
        {
          method: "POST",
          body: JSON.stringify({
            manifest,
            artifact_base64: artifactBase64,
          }),
        },
      );
      setMessage(`模块 ${installed.name}@${installed.version} 已安装并通过健康检查`);
      setModuleManifest(null);
      setModuleArtifact(null);
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "模块安装失败");
    } finally {
      setBusy(null);
    }
  }

  async function proxyAction(action: "refresh_subscription" | "health_check") {
    setBusy(action);
    setMessage(null);
    try {
      setProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/actions", {
          method: "POST",
          body: JSON.stringify({ action }),
        }),
      );
      setMessage(action === "health_check" ? "节点延迟已更新" : "订阅已刷新");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "代理操作失败");
    } finally {
      setBusy(null);
    }
  }

  async function selectNode(name: string) {
    setBusy("node");
    setMessage(null);
    try {
      setProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/selection", {
          method: "PUT",
          body: JSON.stringify({ name }),
        }),
      );
      setMessage(`已切换到 ${name}`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "节点切换失败");
    } finally {
      setBusy(null);
    }
  }

  if (!config) {
    return (
      <Card className="border-border/70 shadow-none">
        <CardContent className="flex h-36 items-center justify-center text-sm text-muted-foreground">
          正在读取 Watch 与代理配置…
        </CardContent>
      </Card>
    );
  }

  return (
    <section className="grid gap-4 xl:grid-cols-2">
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
                setConfig({ ...config, server: event.target.value as "cn" })
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
                  setConfig({
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
                  setConfig({
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

          <div className="rounded-lg border bg-muted/25 p-3">
            <p className="text-xs font-medium">安装热更新模块</p>
            <p className="mt-1 text-xs text-muted-foreground">
              上传 manifest.json 与对应单文件可执行程序；切换前会校验摘要和协议健康状态。
            </p>
            <div className="mt-3 grid gap-2 sm:grid-cols-[1fr_1fr_auto]">
              <Input
                type="file"
                accept="application/json,.json"
                aria-label="选择模块 manifest"
                onChange={(event) =>
                  setModuleManifest(event.target.files?.[0] ?? null)
                }
              />
              <Input
                type="file"
                aria-label="选择模块可执行文件"
                onChange={(event) =>
                  setModuleArtifact(event.target.files?.[0] ?? null)
                }
              />
              <Button
                variant="outline"
                onClick={() => void installModule()}
                disabled={busy !== null}
              >
                安装
              </Button>
            </div>
          </div>

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
                onClick={() =>
                  setConfig({
                    ...config,
                    instances: [...config.instances, newInstance(config.instances)],
                  })
                }
              >
                <Plus className="size-4" />
                新增实例
              </Button>
            </div>

            {config.instances.map((instance, index) => (
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
                      readOnly={savedIds.includes(instance.id)}
                      onChange={(event) =>
                        updateInstance(index, { id: event.target.value })
                      }
                      placeholder="four-player"
                    />
                    <span className="block font-normal text-muted-foreground">
                      {savedIds.includes(instance.id)
                        ? "保存后不可改名，改名会丢弃该实例待抓取的对局"
                        : "只能用字母、数字和 . _ -"}
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
                    <label
                      key={mode}
                      className="flex items-center gap-2 text-sm"
                    >
                      <Checkbox
                        checked={instance.modes.includes(mode)}
                        onCheckedChange={(checked) =>
                          updateInstance(index, {
                            modes: checked
                              ? [...new Set([...instance.modes, mode])]
                              : instance.modes.filter(
                                  (value) => value !== mode,
                                ),
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
                    disabled={config.instances.length < 2}
                    onClick={() =>
                      setConfig({
                        ...config,
                        instances: config.instances.filter(
                          (_, other) => other !== index,
                        ),
                      })
                    }
                  >
                    <Trash2 className="size-4" />
                    删除
                  </Button>
                </div>
              </div>
            ))}
          </div>

          <div className="grid gap-3 sm:grid-cols-[180px_1fr]">
            <label className="space-y-1.5 text-xs font-medium">
              Watch 出站
              <select
                className={selectClass}
                value={config.proxy_mode}
                onChange={(event) =>
                  setConfig({
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
                  setConfig({
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
                  setConfig({ ...config, enabled: checked })
                }
              />
              随后端启动
            </label>
            <span className="text-xs text-muted-foreground">
              总开关；实例还要各自启用才会连接
            </span>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void saveConfig()} disabled={busy !== null}>
              <Save className="size-4" />
              保存并应用
            </Button>
            <Button
              variant="outline"
              onClick={() => void serviceAction("start")}
              disabled={busy !== null}
            >
              <CirclePlay className="size-4" />
              启动
            </Button>
            <Button
              variant="outline"
              onClick={() => void serviceAction("reload")}
              disabled={busy !== null}
            >
              <RotateCw className="size-4" />
              热重载
            </Button>
            <Button
              variant="outline"
              onClick={() => void serviceAction("stop")}
              disabled={busy !== null}
            >
              <CircleStop className="size-4" />
              停止
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/70 shadow-none">
        <CardHeader className="border-b">
          <div className="flex items-start justify-between gap-4">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Cable className="size-4" />
                mihomo 代理
              </CardTitle>
              <CardDescription className="mt-1">
                订阅由后端保密保存，Watch 通过专用策略组出站
              </CardDescription>
            </div>
            <Badge
              variant="outline"
              className={
                proxy?.available
                  ? "border-emerald-200 text-emerald-700"
                  : "border-amber-200 text-amber-700"
              }
            >
              {proxy?.available ? "内核在线" : "内核离线"}
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4 pt-5">
          <div className="rounded-lg border bg-muted/25 p-3 text-xs">
            <div className="flex justify-between gap-3">
              <span className="text-muted-foreground">订阅</span>
              <span className="font-mono">
                {proxy?.subscription_host ?? "未配置"}
              </span>
            </div>
            <div className="mt-2 flex justify-between gap-3">
              <span className="text-muted-foreground">当前节点</span>
              <span className="truncate font-medium">
                {proxy?.selected_node ?? "DIRECT"}
              </span>
            </div>
          </div>

          <label className="block space-y-1.5 text-xs font-medium">
            订阅链接
            <div className="flex gap-2">
              <Input
                type="password"
                autoComplete="off"
                value={subscriptionUrl}
                onChange={(event) => setSubscriptionUrl(event.target.value)}
                placeholder="https://provider.example/sub?token=…"
              />
              <Button
                variant="outline"
                onClick={() => void updateSubscription()}
                disabled={busy !== null}
              >
                更新
              </Button>
            </div>
          </label>

          <label className="block space-y-1.5 text-xs font-medium">
            节点
            <select
              className={selectClass}
              value={proxy?.selected_node ?? "DIRECT"}
              onChange={(event) => void selectNode(event.target.value)}
              disabled={!proxy?.available || busy !== null}
            >
              <option value="DIRECT">DIRECT</option>
              {proxy?.nodes.map((node) => (
                <option key={node.name} value={node.name}>
                  {node.name}
                  {node.delay_ms !== null ? ` · ${node.delay_ms} ms` : ""}
                  {node.alive === false ? " · 不可用" : ""}
                </option>
              ))}
            </select>
          </label>

          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              onClick={() => void proxyAction("refresh_subscription")}
              disabled={!proxy?.subscription_configured || busy !== null}
            >
              <RefreshCw className="size-4" />
              刷新订阅
            </Button>
            <Button
              variant="outline"
              onClick={() => void proxyAction("health_check")}
              disabled={!proxy?.subscription_configured || busy !== null}
            >
              <Gauge className="size-4" />
              测试延迟
            </Button>
          </div>
          {proxy?.error ? (
            <p className="text-xs text-amber-700">{proxy.error}</p>
          ) : null}
        </CardContent>
      </Card>
      {message ? (
        <p className="xl:col-span-2 rounded-lg border bg-background px-3 py-2 text-xs text-muted-foreground">
          {message}
        </p>
      ) : null}
    </section>
  );
}
