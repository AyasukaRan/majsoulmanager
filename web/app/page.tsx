import {
  Activity,
  Archive,
  ArrowDownToLine,
  Boxes,
  CheckCircle2,
  ChevronRight,
  CircleGauge,
  Clock3,
  Database,
  FileJson2,
  Filter,
  HardDrive,
  Layers3,
  MoreHorizontal,
  Search,
  ServerCog,
  UploadCloud,
  Users,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { WatchStatusPanel } from "@/components/watch-status-panel";
import { WatchControlPanel } from "@/components/watch-control-panel";

const records = [
  {
    id: "0197a8d2…6fa1",
    source: "tenhou-east-01",
    players: "Asapin / 多井隆晴 / NAGA / 渋川難波",
    rule: "四麻・東南",
    events: 146,
    size: "6.8 KB",
    time: "刚刚",
  },
  {
    id: "0197a8d1…fd38",
    source: "collector-cn-02",
    players: "星の王子 / player_21 / 雀士A / 雀士B",
    rule: "四麻・东风",
    events: 82,
    size: "4.2 KB",
    time: "12 秒前",
  },
  {
    id: "0197a8cf…21c0",
    source: "league-import",
    players: "KONAMI / 雷電 / 風林火山 / ABEMAS",
    rule: "四麻・東南",
    events: 173,
    size: "8.1 KB",
    time: "34 秒前",
  },
  {
    id: "0197a8cc…9b71",
    source: "tenhou-east-01",
    players: "みかん / 如月 / 九蓮 / リーチ棒",
    rule: "三麻・东南",
    events: 119,
    size: "5.9 KB",
    time: "1 分前",
  },
  {
    id: "0197a8c9…a442",
    source: "collector-cn-01",
    players: "Alpha / Beta / Gamma / Delta",
    rule: "四麻・东南",
    events: 154,
    size: "7.4 KB",
    time: "2 分前",
  },
];

const navItems = [
  { label: "概览", icon: CircleGauge, active: true },
  { label: "对局索引", icon: Database },
  { label: "采集任务", icon: UploadCloud },
  { label: "批量导出", icon: Archive },
];

function MetricCard({
  label,
  value,
  detail,
  icon: Icon,
}: {
  label: string;
  value: string;
  detail: string;
  icon: typeof Database;
}) {
  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="pb-2">
        <CardDescription className="flex items-center justify-between">
          {label}
          <span className="grid size-8 place-items-center rounded-md bg-primary/8 text-primary">
            <Icon className="size-4" />
          </span>
        </CardDescription>
        <CardTitle className="font-mono text-2xl tracking-tight">{value}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-xs text-muted-foreground">{detail}</p>
      </CardContent>
    </Card>
  );
}

export default function Home() {
  return (
    <div className="min-h-svh bg-muted/35">
      <header className="sticky top-0 z-30 border-b bg-background/95 backdrop-blur">
        <div className="mx-auto flex h-16 max-w-[1600px] items-center gap-4 px-4 md:px-6">
          <div className="flex min-w-fit items-center gap-2.5">
            <div className="grid size-8 place-items-center rounded-lg bg-primary text-primary-foreground shadow-sm">
              <Layers3 className="size-[18px]" />
            </div>
            <div>
              <p className="text-sm font-semibold leading-none">mjai 管理台</p>
              <p className="mt-1 font-mono text-[10px] text-muted-foreground">
                DATA CONTROL PLANE
              </p>
            </div>
          </div>
          <Separator orientation="vertical" className="hidden h-6 md:block" />
          <nav className="hidden items-center gap-1 md:flex">
            {navItems.map(({ label, icon: Icon, active }) => (
              <Button
                key={label}
                variant={active ? "secondary" : "ghost"}
                size="sm"
                className="gap-2"
              >
                <Icon className="size-4" />
                {label}
              </Button>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-2">
            <Badge variant="outline" className="hidden gap-1.5 font-normal sm:flex">
              <span className="size-1.5 rounded-full bg-emerald-500" />
              集群正常
            </Badge>
            <Button size="sm" className="gap-2">
              <UploadCloud className="size-4" />
              <span className="hidden sm:inline">导入 mjson</span>
              <span className="sm:hidden">导入</span>
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1600px] space-y-6 px-4 py-6 md:px-6 md:py-8">
        <section className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
          <div>
            <div className="mb-2 flex items-center gap-2 text-xs text-muted-foreground">
              <span>控制台</span>
              <ChevronRight className="size-3" />
              <span className="text-foreground">数据概览</span>
            </div>
            <h1 className="text-2xl font-semibold tracking-tight md:text-3xl">
              对局数据概览
            </h1>
            <p className="mt-1 text-sm text-muted-foreground">
              监控采集、索引与归档状态，快速定位任意一条 mjai 原始记录。
            </p>
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Clock3 className="size-3.5" />
            最后更新：2026-07-23 16:42:08
            <Button variant="outline" size="sm" className="ml-2">
              刷新数据
            </Button>
          </div>
        </section>

        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            label="索引总量"
            value="428,719,506"
            detail="今日新增 2,841,932 条"
            icon={Database}
          />
          <MetricCard
            label="24h 采集速率"
            value="32,893/min"
            detail="峰值 51,204 条/分钟"
            icon={Activity}
          />
          <MetricCard
            label="原始数据"
            value="1.84 TB"
            detail="压缩率 4.7× · 7,391 个 pack"
            icon={HardDrive}
          />
          <MetricCard
            label="待处理队列"
            value="18,420"
            detail="预计 34 秒完成索引"
            icon={Boxes}
          />
        </section>

        <WatchControlPanel />

        <WatchStatusPanel />

        <section className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_340px]">
          <Card className="min-w-0 border-border/70 shadow-none">
            <CardHeader className="gap-4 border-b">
              <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
                <div>
                  <CardTitle>最近入库</CardTitle>
                  <CardDescription className="mt-1">
                    已通过格式校验并完成索引的 mjai 记录
                  </CardDescription>
                </div>
                <Tabs defaultValue="all">
                  <TabsList>
                    <TabsTrigger value="all">全部</TabsTrigger>
                    <TabsTrigger value="4p">四麻</TabsTrigger>
                    <TabsTrigger value="3p">三麻</TabsTrigger>
                  </TabsList>
                </Tabs>
              </div>
              <div className="grid gap-2 sm:grid-cols-[minmax(220px,1fr)_180px_auto]">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    aria-label="搜索记录"
                    placeholder="搜索 UUID、玩家或 SHA-256"
                    className="pl-9"
                  />
                </div>
                <Select defaultValue="all">
                  <SelectTrigger aria-label="选择数据来源">
                    <SelectValue placeholder="全部来源" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部来源</SelectItem>
                    <SelectItem value="tenhou">Tenhou</SelectItem>
                    <SelectItem value="collector">Collector API</SelectItem>
                    <SelectItem value="league">League Import</SelectItem>
                  </SelectContent>
                </Select>
                <Button variant="outline" className="gap-2">
                  <Filter className="size-4" />
                  更多筛选
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="pl-6">记录 ID</TableHead>
                      <TableHead>来源</TableHead>
                      <TableHead className="min-w-64">玩家</TableHead>
                      <TableHead>规则</TableHead>
                      <TableHead className="text-right">事件</TableHead>
                      <TableHead className="text-right">原始大小</TableHead>
                      <TableHead>入库时间</TableHead>
                      <TableHead className="w-12" />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {records.map((record) => (
                      <TableRow key={record.id}>
                        <TableCell className="pl-6 font-mono text-xs font-medium">
                          {record.id}
                        </TableCell>
                        <TableCell>
                          <Badge variant="secondary" className="font-mono font-normal">
                            {record.source}
                          </Badge>
                        </TableCell>
                        <TableCell className="max-w-72 truncate text-sm">
                          {record.players}
                        </TableCell>
                        <TableCell className="whitespace-nowrap text-sm">
                          {record.rule}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs">
                          {record.events}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs">
                          {record.size}
                        </TableCell>
                        <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                          {record.time}
                        </TableCell>
                        <TableCell>
                          <DropdownMenu>
                            <DropdownMenuTrigger
                              render={
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  aria-label={`操作记录 ${record.id}`}
                                />
                              }
                            >
                              <MoreHorizontal className="size-4" />
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuLabel>记录操作</DropdownMenuLabel>
                              <DropdownMenuItem>查看索引详情</DropdownMenuItem>
                              <DropdownMenuItem>预览原始 mjson</DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem>
                                <ArrowDownToLine className="size-4" />
                                下载原始文件
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <div className="flex items-center justify-between border-t px-6 py-4">
                <p className="text-xs text-muted-foreground">
                  显示最近 5 条，共 428,719,506 条记录
                </p>
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" disabled>
                    上一页
                  </Button>
                  <Button variant="outline" size="sm">
                    下一页
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          <aside className="space-y-6">
            <Card className="border-border/70 shadow-none">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <ServerCog className="size-4 text-primary" />
                  服务状态
                </CardTitle>
                <CardDescription>数据链路实时健康状态</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {[
                  ["RustFS", "12.4 ms", "对象存储"],
                  ["ClickHouse", "28.1 ms", "索引查询"],
                  ["Redpanda", "6.8 ms", "采集队列"],
                  ["PostgreSQL", "9.2 ms", "任务元数据"],
                ].map(([name, latency, role]) => (
                  <div key={name} className="flex items-center gap-3">
                    <span className="size-2 rounded-full bg-emerald-500 shadow-[0_0_0_3px_var(--color-emerald-100)]" />
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium">{name}</p>
                      <p className="text-xs text-muted-foreground">{role}</p>
                    </div>
                    <span className="font-mono text-xs text-muted-foreground">
                      {latency}
                    </span>
                  </div>
                ))}
              </CardContent>
            </Card>

            <Card className="border-border/70 shadow-none">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Archive className="size-4 text-primary" />
                  Pack 写入
                </CardTitle>
                <CardDescription>当前活跃分片的打包进度</CardDescription>
                <CardAction>
                  <Badge variant="outline">256 MB</Badge>
                </CardAction>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <div className="mb-2 flex justify-between text-xs">
                    <span>partition-04</span>
                    <span className="font-mono">184.2 / 256 MB</span>
                  </div>
                  <Progress value={72} />
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div className="rounded-lg border bg-muted/35 p-3">
                    <FileJson2 className="mb-2 size-4 text-muted-foreground" />
                    <p className="font-mono text-lg font-semibold">31,842</p>
                    <p className="text-[11px] text-muted-foreground">包内记录</p>
                  </div>
                  <div className="rounded-lg border bg-muted/35 p-3">
                    <CheckCircle2 className="mb-2 size-4 text-muted-foreground" />
                    <p className="font-mono text-lg font-semibold">4.83×</p>
                    <p className="text-[11px] text-muted-foreground">压缩倍率</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="border-primary/20 bg-primary/[0.035] shadow-none">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Users className="size-4 text-primary" />
                  新建批量导出
                </CardTitle>
                <CardDescription>
                  使用当前筛选条件创建异步 tar.gz 导出任务。
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Button className="w-full gap-2">
                  <ArrowDownToLine className="size-4" />
                  创建导出任务
                </Button>
              </CardContent>
            </Card>
          </aside>
        </section>
      </main>
    </div>
  );
}
