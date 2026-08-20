import Link from "next/link";
import { Settings } from "lucide-react";

import { Button } from "@/components/ui/button";
import { WatchLogPanel } from "@/components/watch-log-panel";
import { WatchStatusPanel } from "@/components/watch-status-panel";

export default function WatchPage() {
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs text-muted-foreground">控制台 / Watch 服务</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">雀魂实时采集</h1>
          <p className="mt-1 text-sm text-muted-foreground">查看后台服务状态、逐条转换进度与实时日志。</p>
        </div>
        {/* The configuration moved off this page, so the way to it has to stay
            on the page it left, or it is only findable by guessing. */}
        <Button variant="outline" render={<Link href="/settings" />}>
          <Settings className="size-4" />
          采集与代理设置
        </Button>
      </div>
      {/* Status first: whether the service is running is what an operator opens
          this page to learn, and the log below it explains whatever it says. */}
      <WatchStatusPanel />
      {/* This page's own services, not every service. The re-fetch pool runs
          eighty-odd sessions through the same 500-entry ring, so an unfiltered
          panel here shows the collectors' last few lines and then nothing but
          补抓 for as long as a sweep lasts. */}
      <WatchLogPanel
        source={["service", "collector:", "module"]}
        description="采集服务、各采集实例与协议模块的最近日志（内存环形缓冲，最多保留 500 条）；补抓的日志在牌谱补抓页"
      />
    </div>
  );
}
