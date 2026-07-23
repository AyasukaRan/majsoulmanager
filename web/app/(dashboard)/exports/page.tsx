import { Archive, Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";

export default function ExportsPage() {
  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between"><div><p className="text-xs text-muted-foreground">控制台 / 批量导出</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">批量导出</h1><p className="mt-1 text-sm text-muted-foreground">根据筛选快照异步生成大规模归档。</p></div><Button><Archive className="size-4" />新建导出</Button></div>
      <section className="grid gap-4">
        {[["2026-07 玉之间四麻", 76, "18,420,000 / 24,000,000"], ["最近 7 天 Watch 数据", 100, "2,841,932 / 2,841,932"]].map(([name, progress, detail]) => <Card key={String(name)} className="shadow-none"><CardHeader><div className="flex items-center justify-between"><div><CardTitle>{name}</CardTitle><CardDescription className="mt-1">{detail} 条</CardDescription></div><Button variant="outline" disabled={progress !== 100}><Download className="size-4" />下载</Button></div></CardHeader><CardContent><Progress value={Number(progress)} /></CardContent></Card>)}
      </section>
    </div>
  );
}
