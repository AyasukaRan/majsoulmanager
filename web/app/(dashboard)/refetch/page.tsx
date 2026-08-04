import { PaipuyaGapCard } from "@/components/paipuya-gap";
import { RefetchPanel } from "@/components/refetch-panel";
import { WatchLogPanel } from "@/components/watch-log-panel";

export default function RefetchPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 牌谱补抓</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">历史牌谱补抓</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          转换器修好之前入库的记录没有雀魂原始牌谱，mjai 也带着已经修掉的那些错。
          这里用一批独立账号按 uuid 把它们重新抓回来、用当前转换器重转，并替换索引里那一行。
        </p>
      </div>
      <PaipuyaGapCard />
      <RefetchPanel />
      <WatchLogPanel
        source="refetch"
        title="补抓日志"
        description="只显示补抓服务与它各个会话的日志；采集实例的日志在 Watch 服务页"
        emptyHint="补抓启动后日志会自动出现在这里"
      />
    </div>
  );
}
