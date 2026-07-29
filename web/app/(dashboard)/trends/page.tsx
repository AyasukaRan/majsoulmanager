import { StatsTrends } from "@/components/stats-trends";

export default function TrendsPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 数据趋势</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">数据趋势</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          概览给的是当下的四个数字，这里给的是它们怎么走到今天的。
        </p>
      </div>
      <StatsTrends />
    </div>
  );
}
