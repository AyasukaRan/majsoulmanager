"use client";

import { useState } from "react";
import { Cable, Gauge, RefreshCw } from "lucide-react";

import { jsonRequest, type MihomoStatus } from "@/lib/mjai-api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { selectClass } from "@/lib/utils";
import type { RunAction } from "@/components/watch/busy-action";

export function WatchProxyCard({
  proxy,
  onProxy,
  busy,
  run,
  onMessage,
}: {
  proxy: MihomoStatus | null;
  onProxy: (next: MihomoStatus) => void;
  busy: boolean;
  run: RunAction;
  onMessage: (message: string) => void;
}) {
  const [subscriptionUrl, setSubscriptionUrl] = useState("");

  async function updateSubscription() {
    if (!subscriptionUrl) {
      onMessage("请输入订阅链接");
      return;
    }
    await run("订阅更新失败", async () => {
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
      onProxy(status);
      setSubscriptionUrl("");
      return "订阅已写入私密配置并刷新节点";
    });
  }

  async function proxyAction(action: "refresh_subscription" | "health_check") {
    await run("代理操作失败", async () => {
      onProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/actions", {
          method: "POST",
          body: JSON.stringify({ action }),
        }),
      );
      return action === "health_check" ? "节点延迟已更新" : "订阅已刷新";
    });
  }

  async function selectNode(name: string) {
    await run("节点切换失败", async () => {
      onProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/selection", {
          method: "PUT",
          body: JSON.stringify({ name }),
        }),
      );
      return `已切换到 ${name}`;
    });
  }

  return (
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
              disabled={busy}
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
            disabled={!proxy?.available || busy}
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
            disabled={!proxy?.subscription_configured || busy}
          >
            <RefreshCw className="size-4" />
            刷新订阅
          </Button>
          <Button
            variant="outline"
            onClick={() => void proxyAction("health_check")}
            disabled={!proxy?.subscription_configured || busy}
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
  );
}
