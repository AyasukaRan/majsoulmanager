import { Search } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

const rows = [
  ["0197a8d2…6fa1", "majsoul-watch", "Asapin / 多井隆晴 / NAGA / 渋川難波", "四麻·东南", "146", "刚刚"],
  ["0197a8d1…fd38", "collector-cn-02", "星の王子 / player_21 / 雀士A / 雀士B", "四麻·东风", "82", "12 秒前"],
  ["0197a8cf…21c0", "league-import", "KONAMI / 雷電 / 風林火山 / ABEMAS", "四麻·东南", "173", "34 秒前"],
];

export default function RecordsPage() {
  return (
    <div className="space-y-6">
      <div><p className="text-xs text-muted-foreground">控制台 / 牌谱索引</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">牌谱索引</h1><p className="mt-1 text-sm text-muted-foreground">筛选、定位并读取任意单条 mjson。</p></div>
      <Card className="shadow-none">
        <CardHeader className="border-b"><CardTitle>索引记录</CardTitle><CardDescription>使用 UUID、玩家、来源和时间范围组合筛选</CardDescription><div className="relative mt-3 max-w-xl"><Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" /><Input className="pl-9" placeholder="搜索 UUID、玩家或 SHA-256" /></div></CardHeader>
        <CardContent className="p-0"><Table><TableHeader><TableRow><TableHead className="pl-6">记录 ID</TableHead><TableHead>来源</TableHead><TableHead>玩家</TableHead><TableHead>规则</TableHead><TableHead>事件</TableHead><TableHead>入库时间</TableHead></TableRow></TableHeader><TableBody>{rows.map((row) => <TableRow key={row[0]}>{row.map((cell, index) => <TableCell key={index} className={index === 0 ? "pl-6 font-mono text-xs" : ""}>{index === 1 ? <Badge variant="secondary">{cell}</Badge> : cell}</TableCell>)}</TableRow>)}</TableBody></Table></CardContent>
      </Card>
    </div>
  );
}
