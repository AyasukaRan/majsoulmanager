import { redirect } from "next/navigation";
import { WatchSettings } from "@/components/watch/settings";
import { requireSessionUser } from "@/lib/session";

export default async function SettingsPage() {
  const user = await requireSessionUser();
  // Guarded like 用户管理, and for a stronger reason: this page takes a proxy
  // subscription and the account secret refs, and a single save stops and
  // restarts a live collection. The backend route behind it still only asks for
  // a session, so this is the console drawing the line, not authorisation.
  if (user.role !== "admin") redirect("/");
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 设置</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">采集与代理设置</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          配置采集实例、协议模块与出站代理；保存后在线重载，不重启管理 API。
        </p>
      </div>
      <WatchSettings />
    </div>
  );
}
