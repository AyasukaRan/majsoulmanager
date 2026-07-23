import { Activity, Archive, Database, HardDrive, Radio } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const metrics = [
  ["索引总量", "428,719,506", "今日新增 2,841,932 条", Database],
  ["24h 采集速率", "32,893/min", "峰值 51,204 条/分钟", Activity],
  ["原始数据", "1.84 TB", "压缩率 4.7× · 7,391 个 pack", HardDrive],
  ["待导出任务", "12", "3 个任务正在运行", Archive],
] as const;

export default function OverviewPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 概览</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">数据运行概览</h1>
        <p className="mt-1 text-sm text-muted-foreground">这里只保留需要快速判断的关键状态，详细操作已拆分到独立页面。</p>
      </div>
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {metrics.map(([label, value, detail, Icon]) => (
          <Card key={label} className="shadow-none">
            <CardHeader className="pb-2">
              <CardDescription className="flex items-center justify-between">{label}<Icon className="size-4 text-primary" /></CardDescription>
              <CardTitle className="font-mono text-2xl">{value}</CardTitle>
            </CardHeader>
            <CardContent><p className="text-xs text-muted-foreground">{detail}</p></CardContent>
          </Card>
        ))}
      </section>
      <section className="grid gap-4 lg:grid-cols-2">
        <Card className="shadow-none">
          <CardHeader><CardTitle className="flex items-center gap-2"><Radio className="size-4" />Watch 摘要</CardTitle><CardDescription>实时采集与转换服务</CardDescription></CardHeader>
          <CardContent className="grid grid-cols-3 gap-3">
            {["直播 UUID|24", "待获取|18", "转换失败|2"].map((item) => {
              const [label, value] = item.split("|");
              return <div key={label} className="rounded-lg border bg-muted/30 p-3"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 font-mono text-xl font-semibold">{value}</p></div>;
            })}
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardHeader><CardTitle>系统状态</CardTitle><CardDescription>核心依赖健康概览</CardDescription></CardHeader>
          <CardContent className="space-y-3">
            {["Rust API", "RustFS", "ClickHouse", "PostgreSQL"].map((name) => <div key={name} className="flex items-center justify-between border-b pb-2 text-sm last:border-0"><span>{name}</span><span className="text-xs text-emerald-600">● 正常</span></div>)}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
