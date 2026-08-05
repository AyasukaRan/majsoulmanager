import { PaipuyaGapCard } from "@/components/paipuya-gap";
import { PaipuyaSyncCard } from "@/components/paipuya-sync";
import { RefetchPanel } from "@/components/refetch-panel";
import { WatchLogPanel } from "@/components/watch-log-panel";

export default function RefetchPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 牌谱补抓</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">历史牌谱补抓</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          一批独立账号，按 uuid 向雀魂要牌谱。两种活：把转换器修好之前入库、没有原始牌谱的那些记录重抓重转，
          或者走查牌谱屋的收录，把本地没有的对局抓回来。后者先在本地比对，没存过的才发请求——
          雀魂那边的额度是这里唯一花不起的东西。
        </p>
      </div>
      <PaipuyaSyncCard />
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
