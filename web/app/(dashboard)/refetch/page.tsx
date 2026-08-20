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
      {/* Not just `refetch`. `paipuya` is the two cards above this one — the
          牌谱屋 sync and the gap comparison — whose lines were readable only on
          the Watch page, beside services this page has nothing to do with. And
          `backfill` is the one the sweep refuses to start without: it tells the
          operator to search the log for 「对局幂等认领」, which has to be a log
          this page shows. */}
      <WatchLogPanel
        source={["refetch", "paipuya", "backfill"]}
        title="补抓日志"
        description="只显示补抓服务、它各个会话、牌谱屋同步与幂等回填的日志；采集实例的日志在 Watch 服务页"
        emptyHint="补抓启动后日志会自动出现在这里"
      />
    </div>
  );
}
