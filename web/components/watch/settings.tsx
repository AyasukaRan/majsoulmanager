"use client";

import { useCallback, useEffect, useState } from "react";

import {
  jsonRequest,
  type InstalledWatchModule,
  type MihomoStatus,
  type WatchServiceConfig,
} from "@/lib/mjai-api";
import { Card, CardContent } from "@/components/ui/card";
import { useBusyAction } from "@/components/watch/busy-action";
import { WatchProxyCard } from "@/components/watch/proxy-card";
import { WatchServiceCard } from "@/components/watch/service-card";

/**
 * Everything that drives the collectors, kept off the monitoring page so that
 * watching a live collection cannot turn into editing it by one stray click.
 * The three readings arrive together because a module list and a proxy status
 * are only meaningful next to the configuration that selects from them.
 */
export function WatchSettings() {
  const [config, setConfig] = useState<WatchServiceConfig | null>(null);
  const [modules, setModules] = useState<InstalledWatchModule[]>([]);
  const [proxy, setProxy] = useState<MihomoStatus | null>(null);
  const { busy, message, setMessage, run } = useBusyAction();

  const refresh = useCallback(async () => {
    try {
      const [nextConfig, nextModules, nextProxy] = await Promise.all([
        jsonRequest<WatchServiceConfig>("/api/watch/config"),
        jsonRequest<InstalledWatchModule[]>("/api/watch/modules"),
        jsonRequest<MihomoStatus>("/api/watch/proxy"),
      ]);
      setConfig(nextConfig);
      setModules(nextModules);
      setProxy(nextProxy);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "读取配置失败");
    }
  }, [setMessage]);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    return () => window.clearTimeout(initial);
  }, [refresh]);

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
      <WatchServiceCard
        config={config}
        onChange={setConfig}
        modules={modules}
        busy={busy}
        run={run}
        onMessage={setMessage}
        onReload={refresh}
      />
      <WatchProxyCard
        proxy={proxy}
        onProxy={setProxy}
        busy={busy}
        run={run}
        onMessage={setMessage}
      />
      {message ? (
        <p className="xl:col-span-2 rounded-lg border bg-background px-3 py-2 text-xs text-muted-foreground">
          {message}
        </p>
      ) : null}
    </section>
  );
}
