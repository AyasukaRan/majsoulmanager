"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Activity, Archive, Database, HardDrive, Radio } from "lucide-react";

import { jsonRequest, type StatsSummary } from "@/lib/mjai-api";
import { formatBytes } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

const PHASE_LABELS: Record<string, string> = {
  stopped: "已停止",
  starting: "启动中",
  running: "运行中",
  reloading: "重载中",
  stopping: "停止中",
  failed: "已失败",
};

function count(value: number) {
  return value.toLocaleString("zh-CN");
}

export function StatsOverview() {
  const [stats, setStats] = useState<StatsSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const inFlightRef = useRef(false);

  const refresh = useCallback(async () => {
    if (inFlightRef.current) {
      return;
    }
    inFlightRef.current = true;
    try {
      setStats(await jsonRequest<StatsSummary>("/api/stats"));
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "读取统计失败");
    } finally {
      inFlightRef.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(), 15_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  // Numbers stay blank rather than falling back to zero: "0 条" and "读不到"
  // are different operational situations and must not look alike.
  const metrics = stats
    ? [
        {
          label: "索引总量",
          value: count(stats.records.total),
          detail: `近 24 小时新增 ${count(stats.records.last_24h)} 条`,
          icon: Database,
        },
        {
          label: "24 小时入库",
          value: count(stats.records.last_24h),
          detail: `来源 ${stats.records.sources.length} 个`,
          icon: Activity,
        },
        {
          label: "原始数据",
          value: formatBytes(stats.storage.raw_bytes),
          detail: `压缩后 ${formatBytes(stats.storage.compressed_bytes)} · ${count(stats.storage.packs)} 个 pack`,
          icon: HardDrive,
        },
        {
          label: "导出任务",
          value: count(stats.downloads.queued + stats.downloads.running),
          detail: `已完成 ${count(stats.downloads.completed)} · 失败 ${count(stats.downloads.failed)}`,
          icon: Archive,
        },
      ]
    : [
        { label: "索引总量", value: "—", detail: "", icon: Database },
        { label: "24 小时入库", value: "—", detail: "", icon: Activity },
        { label: "原始数据", value: "—", detail: "", icon: HardDrive },
        { label: "导出任务", value: "—", detail: "", icon: Archive },
      ];

  const placeholder = loading ? "正在读取统计数据…" : "统计数据不可用";

  return (
    <>
      {error ? (
        <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
          无法读取统计数据：{error}
        </p>
      ) : null}
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {metrics.map(({ label, value, detail, icon: Icon }) => (
          <Card key={label} className="shadow-none">
            <CardHeader className="pb-2">
              <CardDescription className="flex items-center justify-between">
                {label}
                <Icon className="size-4 text-primary" />
              </CardDescription>
              <CardTitle className="font-mono text-2xl">{value}</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-xs text-muted-foreground">
                {detail || placeholder}
              </p>
            </CardContent>
          </Card>
        ))}
      </section>
      <section className="grid gap-4 lg:grid-cols-2">
        <Card className="shadow-none">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Radio className="size-4" />
              Watch 摘要
              {stats ? (
                <Badge variant="outline">
                  {PHASE_LABELS[stats.watch.phase] ?? stats.watch.phase}
                </Badge>
              ) : null}
            </CardTitle>
            <CardDescription>实时采集与转换服务</CardDescription>
          </CardHeader>
          <CardContent className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            {(
              [
                ["直播 UUID", stats?.watch.live],
                ["待获取", stats?.watch.pending],
                ["已完成", stats?.watch.completed],
                ["转换失败", stats?.watch.failed],
              ] as const
            ).map(([label, value]) => (
              <div key={label} className="rounded-lg border bg-muted/30 p-3">
                <p className="text-xs text-muted-foreground">{label}</p>
                <p className="mt-1 font-mono text-xl font-semibold">
                  {value === undefined ? "—" : count(value)}
                </p>
              </div>
            ))}
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardHeader>
            <CardTitle>来源分布</CardTitle>
            <CardDescription>各采集来源的入库条数</CardDescription>
          </CardHeader>
          <CardContent className="space-y-2">
            {stats?.records.sources.length ? (
              stats.records.sources.map(({ source, records }) => (
                <div
                  key={source}
                  className="flex items-center justify-between border-b pb-2 text-sm last:border-0"
                >
                  <span className="truncate">{source}</span>
                  <span className="font-mono text-xs text-muted-foreground">
                    {count(records)}
                  </span>
                </div>
              ))
            ) : (
              <p className="py-6 text-center text-sm text-muted-foreground">
                {stats ? "暂无入库来源" : placeholder}
              </p>
            )}
          </CardContent>
        </Card>
      </section>
    </>
  );
}
