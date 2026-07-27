import { StatsOverview } from "@/components/stats-overview";

export default function OverviewPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 概览</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">
          数据运行概览
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          这里只保留需要快速判断的关键状态，详细操作已拆分到独立页面。
        </p>
      </div>
      <StatsOverview />
    </div>
  );
}
