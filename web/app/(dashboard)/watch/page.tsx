import { WatchControlPanel } from "@/components/watch-control-panel";
import { WatchLogPanel } from "@/components/watch-log-panel";
import { WatchStatusPanel } from "@/components/watch-status-panel";

export default function WatchPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / Watch 服务</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">雀魂实时采集</h1>
        <p className="mt-1 text-sm text-muted-foreground">管理采集任务、热更新协议模块、代理节点及逐条转换状态。</p>
      </div>
      <WatchControlPanel />
      <WatchLogPanel />
      <WatchStatusPanel />
    </div>
  );
}
