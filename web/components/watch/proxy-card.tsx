"use client";

import { useState } from "react";
import { Cable, Gauge, RefreshCw, Trash2 } from "lucide-react";

import {
  jsonRequest,
  type MihomoLane,
  type MihomoStatus,
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
import { Input } from "@/components/ui/input";
import { cn, formatBytes, selectClass } from "@/lib/utils";
import type { RunAction } from "@/components/watch/busy-action";

const LANE_LABELS: Record<MihomoLane, string> = {
  watch: "实时采集",
  refetch: "批量补抓",
};

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
  const [subscriptionLabel, setSubscriptionLabel] = useState("");
  const subscriptions = proxy?.subscriptions ?? [];
  const healthy = (proxy?.nodes ?? []).filter(
    (node) => node.alive === true,
  ).length;

  async function addSubscription() {
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
            label: subscriptionLabel.trim() || null,
            // Per subscription, not per deployment. The box for it lives with
            // the link because that is the thing it belongs to.
            update_interval_secs: 3600,
          }),
        },
      );
      onProxy(status);
      setSubscriptionUrl("");
      setSubscriptionLabel("");
      return "订阅已写入私密配置并刷新节点";
    });
  }

  async function removeSubscription(id: string, label: string) {
    await run("删除订阅失败", async () => {
      onProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/actions", {
          method: "POST",
          body: JSON.stringify({ action: { remove_subscription: { id } } }),
        }),
      );
      return `已删除订阅 ${label}；绑在它节点上的账号会回到补抓出站`;
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
      return action === "health_check"
        ? "已重测每个节点能不能连上雀魂"
        : "订阅已刷新";
    });
  }

  async function selectNode(lane: MihomoLane, name: string) {
    await run("节点切换失败", async () => {
      onProxy(
        await jsonRequest<MihomoStatus>("/api/watch/proxy/selection", {
          method: "PUT",
          body: JSON.stringify({ lane, name }),
        }),
      );
      return `${LANE_LABELS[lane]}已切换到 ${name}`;
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
              订阅由后端保密保存，可以加多条、节点合成一个池子。健康检查探的是雀魂本身，
              不是能不能上网
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
            <span className="text-muted-foreground">节点池</span>
            <span className="font-medium">
              {subscriptions.length === 0
                ? "未配置订阅"
                : `${proxy?.nodes.length ?? 0} 个节点，${healthy} 个能连上雀魂`}
            </span>
          </div>
          <div className="mt-2 flex justify-between gap-3">
            <span className="text-muted-foreground">全局</span>
            <span className="truncate font-medium">
              {proxy?.selected_node ?? "DIRECT"}
            </span>
          </div>
          {(proxy?.lanes ?? []).map((lane) => (
            <div key={lane.lane} className="mt-2 flex justify-between gap-3">
              <span className="text-muted-foreground">
                {LANE_LABELS[lane.lane]}
              </span>
              <span className="truncate font-medium">
                {!lane.available
                  ? "分组未生效"
                  : lane.follows_shared
                    ? `跟随全局（${lane.effective_node ?? "DIRECT"}）`
                    : (lane.selected_node ?? "DIRECT")}
              </span>
            </div>
          ))}
        </div>

        {/* One row per subscription. No link, ever — a subscription URL is the
            whole of the operator's account with that provider, so the backend
            answers with the host and nothing else. The node counts are what an
            operator actually needs from this list: a provider contributing zero
            healthy nodes is one to stop paying for. */}
        {subscriptions.length > 0 ? (
          <div className="space-y-1.5 rounded-lg border bg-muted/25 p-3">
            {subscriptions.map((subscription) => (
              <div
                key={subscription.id}
                className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs"
              >
                <span className="font-medium">{subscription.label}</span>
                <span className="font-mono text-muted-foreground">
                  {subscription.host ?? "—"}
                </span>
                <span
                  className={
                    subscription.healthy > 0
                      ? "text-muted-foreground"
                      : "text-amber-700"
                  }
                >
                  {subscription.nodes} 个节点 · {subscription.healthy} 个能连雀魂
                </span>
                <Button
                  variant="ghost"
                  size="sm"
                  className="ml-auto h-7 gap-1 px-2 text-muted-foreground"
                  onClick={() =>
                    void removeSubscription(subscription.id, subscription.label)
                  }
                  disabled={busy}
                >
                  <Trash2 className="size-3.5" />
                  删除
                </Button>
              </div>
            ))}
          </div>
        ) : null}

        <div className="space-y-1.5 text-xs font-medium">
          添加订阅
          <div className="flex gap-2">
            <Input
              className="max-w-32"
              autoComplete="off"
              value={subscriptionLabel}
              onChange={(event) => setSubscriptionLabel(event.target.value)}
              placeholder="名称（可选）"
              aria-label="订阅名称"
            />
            <Input
              type="password"
              autoComplete="off"
              value={subscriptionUrl}
              onChange={(event) => setSubscriptionUrl(event.target.value)}
              placeholder="https://provider.example/sub?token=…"
              aria-label="订阅链接"
            />
            <Button
              variant="outline"
              onClick={() => void addSubscription()}
              disabled={busy}
            >
              添加
            </Button>
          </div>
          <span className="block font-normal text-muted-foreground">
            第二条起的节点会自动加上前缀，免得两家都叫「香港 01」的节点撞在一起
          </span>
        </div>

        {/* One picker per half. They are separate because the two log in with
            different accounts and behave visibly differently — the re-fetch pool
            is the half that looks like a script — and putting both on one exit
            is a choice, not a default worth hiding. A lane whose group mihomo
            did not accept says so rather than offering a picker that changes
            nothing. */}
        {(proxy?.lanes ?? []).map((lane) => (
          <label key={lane.lane} className="block space-y-1.5 text-xs font-medium">
            {LANE_LABELS[lane.lane]}出站节点
            <select
              className={selectClass}
              // "跟随全局" when mihomo has no group for this lane either: the
              // half really is going out through the shared port and the shared
              // group, which is what that option means.
              value={lane.selected_node ?? "MAJSOUL"}
              onChange={(event) => void selectNode(lane.lane, event.target.value)}
              disabled={!proxy?.available || !lane.available || busy}
            >
              {/* The default, and the reason an upgrade changes nothing: a lane
                  that has never been picked follows the group the deployment
                  was already on. */}
              <option value="MAJSOUL">
                跟随全局（{proxy?.selected_node ?? "DIRECT"}）
              </option>
              <option value="DIRECT">DIRECT（本机出口）</option>
              {proxy?.nodes.map((node) => (
                <option key={node.name} value={node.name}>
                  {node.name}
                  {node.delay_ms !== null ? ` · ${node.delay_ms} ms` : ""}
                  {/* Three states, not two. `null` is "never checked", which
                      the pool treats as unusable — showing it as if it were
                      fine is how an unprobed node gets picked by hand. */}
                  {node.alive === false
                    ? " · 连不上雀魂"
                    : node.alive === null
                      ? " · 未探测"
                      : ""}
                </option>
              ))}
            </select>
            <span className="block font-normal text-muted-foreground">
              {lane.available
                ? `走 ${lane.proxy_url}`
                : `mihomo 里还没有 ${lane.group} 这个分组，这一半仍然走 ${proxy?.proxy_url ?? "共用出站"}——和分流之前一样，不会断`}
            </span>
          </label>
        ))}

        {/* Read-only on purpose: which node an account goes out of is decided
            on the 账号池 page, one row at a time, and a second place to change
            it would be a second answer to the same question. What belongs here
            is whether mihomo actually grew the listener that binding needs. */}
        {(proxy?.outbounds ?? []).length > 0 ? (
          <div className="space-y-1.5 rounded-lg border bg-muted/25 p-3">
            <p className="text-xs font-medium">
              {"补抓池的独立出站（账号池里绑到某个节点的账号走这些）"}
            </p>
            {(proxy?.outbounds ?? []).map((outbound) => (
              <p
                key={outbound.group}
                className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground"
              >
                <span className="font-mono">{outbound.node}</span>
                <span className="font-mono">{outbound.proxy_url}</span>
                <span>
                  {outbound.available &&
                  outbound.selected_node === outbound.node
                    ? "已生效"
                    : `还没生效，绑在它上面的账号先走 ${
                        proxy?.lanes.find((lane) => lane.lane === "refetch")
                          ?.proxy_url ?? "共用出站"
                      }`}
                </span>
              </p>
            ))}
          </div>
        ) : null}

        {/* Traffic, because this walk is not a small consumer: 191 million
            games at 52KiB is nine terabytes at par, and a subscription bills
            that times the multiplier written into the node's own name. Sorted
            by what each node has actually spent, which is the order the quota
            runs out in. */}
        {(proxy?.nodes ?? []).length === 0 ? null : (
          <details className="rounded-lg border bg-muted/25 px-3 py-2">
            <summary className="cursor-pointer text-xs text-muted-foreground">
              {"节点流量与倍率 —— 合计 "}
              <span className="font-mono">
                {formatBytes(
                  (proxy?.nodes ?? []).reduce((sum, node) => sum + node.bytes, 0),
                )}
              </span>
              {"，按倍率折算 "}
              <span className="font-mono">
                {formatBytes(
                  (proxy?.nodes ?? []).reduce(
                    (sum, node) => sum + node.bytes * node.multiplier,
                    0,
                  ),
                )}
              </span>
              {`；倍率高于 ${proxy?.max_multiplier ?? 2} 的不分配账号`}
            </summary>
            <div className="mt-2 max-h-72 space-y-1 overflow-y-auto text-xs">
              {[...(proxy?.nodes ?? [])]
                .sort((left, right) => right.bytes - left.bytes)
                .map((node) => {
                  const skipped = node.multiplier > (proxy?.max_multiplier ?? 2);
                  return (
                    <div
                      key={node.name}
                      className="flex items-baseline justify-between gap-3 border-b pb-1 last:border-0"
                    >
                      <span
                        className={cn(
                          "min-w-0 truncate",
                          skipped && "text-muted-foreground line-through",
                        )}
                        title={skipped ? "倍率过高，不给它分配账号" : undefined}
                      >
                        {node.name}
                      </span>
                      <span className="shrink-0 font-mono tabular-nums text-muted-foreground">
                        {node.multiplier !== 1 ? (
                          <span
                            className={cn(
                              "mr-2",
                              node.multiplier > 1
                                ? "text-amber-600 dark:text-amber-400"
                                : "text-emerald-600 dark:text-emerald-400",
                            )}
                          >
                            ×{node.multiplier}
                          </span>
                        ) : null}
                        <span className="font-semibold text-foreground">
                          {formatBytes(node.bytes)}
                        </span>
                        {node.multiplier !== 1
                          ? ` → ${formatBytes(node.bytes * node.multiplier)}`
                          : ""}
                      </span>
                    </div>
                  );
                })}
            </div>
          </details>
        )}

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
            重测节点
          </Button>
        </div>
        {proxy?.error ? (
          <p className="text-xs text-amber-700">{proxy.error}</p>
        ) : null}
      </CardContent>
    </Card>
  );
}
