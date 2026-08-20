import { AccountPoolCard } from "@/components/account-pool";
import { AccountRegisterCard } from "@/components/account-register";
import { WatchLogPanel } from "@/components/watch-log-panel";

export default function AccountsPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 账号池</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">雀魂账号池</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          这个部署用来登录雀魂的账号，按用途分成两拨：实时采集一个实例一个账号，
          批量补抓有几个账号就开几个会话。加在这里之后，去 Watch 设置和牌谱补抓页把它们选上。
        </p>
      </div>
      <AccountPoolCard />
      {/* Below the pool, not above: registering is the occasional thing, and
          what an operator opens this page for is the list. */}
      <AccountRegisterCard />
      {/* Registration's own lines. They used to be readable only on the Watch
          page, mixed into the collectors' — and now that each page filters to
          its own services, this is the only place they appear. */}
      <WatchLogPanel
        source="register"
        title="注册日志"
        description="只显示批量注册的日志（内存环形缓冲，最多保留 500 条）"
        emptyHint="开始注册后日志会自动出现在这里"
      />
    </div>
  );
}
