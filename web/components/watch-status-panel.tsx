"use client";

import { useCallback, useEffect, useState } from "react";
import {
  CircleCheck,
  CircleX,
  LoaderCircle,
  Radio,
  RefreshCw,
  TimerReset,
} from "lucide-react";

import type {
  WatchConversionState,
  WatchRecord,
  WatchSummary,
  WatchUuidState,
} from "@/lib/mjai-api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const EMPTY_STATUS: WatchSummary = {
  total: 0,
  live: 0,
  pending: 0,
  converting: 0,
  completed: 0,
  failed: 0,
  items: [],
  service: {
    phase: "stopped",
    active_revision: null,
    login_module: null,
    pb_fetch_module: null,
    started_at: null,
    updated_at: new Date(0).toISOString(),
    last_error: null,
  },
};

const UUID_LABELS: Record<WatchUuidState, string> = {
  live: "直播中",
  pending: "待获取",
  fetching: "获取中",
  fetched: "已获取",
  failed: "获取失败",
};

const CONVERSION_LABELS: Record<WatchConversionState, string> = {
  waiting: "等待转换",
  converting: "转换中",
  completed: "转换完成",
  failed: "转换失败",
};

const MODE_LABELS: Record<number, string> = {
  8: "金之间·东风",
  9: "金之间·东南",
  11: "玉之间·东风",
  12: "玉之间·东南",
  15: "王座之间·东风",
  16: "王座之间·东南",
  21: "三麻金·东风",
  22: "三麻金·东南",
  23: "三麻玉·东风",
  24: "三麻玉·东南",
  25: "三麻王座·东风",
  26: "三麻王座·东南",
};

function stateClass(state: WatchUuidState | WatchConversionState) {
  switch (state) {
    case "live":
      return "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-900 dark:bg-sky-950 dark:text-sky-300";
    case "fetching":
    case "converting":
      return "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300";
    case "completed":
    case "fetched":
      return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-300";
    case "failed":
      return "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300";
    default:
      return "border-border bg-muted text-muted-foreground";
  }
}

function StatusBadge({
  state,
  labels,
}: {
  state: WatchUuidState | WatchConversionState;
  labels: Record<string, string>;
}) {
  return (
    <Badge variant="outline" className={stateClass(state)}>
      {labels[state]}
    </Badge>
  );
}

function formatTime(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    hour12: false,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

export function WatchStatusPanel() {
  const [status, setStatus] = useState<WatchSummary>(EMPTY_STATUS);
  const [loading, setLoading] = useState(true);
  const [available, setAvailable] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/api/watch/status?limit=20", {
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error("watch status unavailable");
      }
      setStatus((await response.json()) as WatchSummary);
      setAvailable(true);
    } catch {
      setAvailable(false);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(), 3_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  const stats = [
    { label: "直播 UUID", value: status.live, icon: Radio },
    { label: "待获取", value: status.pending, icon: TimerReset },
    { label: "转换中", value: status.converting, icon: LoaderCircle },
    { label: "已完成", value: status.completed, icon: CircleCheck },
    { label: "失败", value: status.failed, icon: CircleX },
  ];

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="gap-4 border-b">
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
          <div>
            <div className="flex items-center gap-2">
              <CardTitle>Majsoul Watch 实时转换</CardTitle>
              <Badge
                variant="outline"
                className={
                  available
                    ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                    : "border-amber-200 bg-amber-50 text-amber-700"
                }
              >
                <span
                  className={`mr-1.5 size-1.5 rounded-full ${
                    available ? "bg-emerald-500" : "bg-amber-500"
                  }`}
                />
                {available ? "实时同步" : "等待状态服务"}
              </Badge>
            </div>
            <CardDescription className="mt-1">
              {status.service.phase === "running"
                ? `后台服务运行中 · 配置版本 r${status.service.active_revision ?? "—"}`
                : `后台服务：${status.service.phase}`}
            </CardDescription>
          </div>
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => void refresh()}
            disabled={loading}
          >
            <RefreshCw className={`size-3.5 ${loading ? "animate-spin" : ""}`} />
            刷新
          </Button>
        </div>
        <div className="grid grid-cols-2 gap-2 md:grid-cols-5">
          {stats.map(({ label, value, icon: Icon }) => (
            <div key={label} className="rounded-lg border bg-muted/25 px-3 py-2.5">
              <div className="flex items-center justify-between text-xs text-muted-foreground">
                {label}
                <Icon className="size-3.5" />
              </div>
              <p className="mt-1 font-mono text-xl font-semibold">
                {value.toLocaleString("zh-CN")}
              </p>
            </div>
          ))}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">UUID</TableHead>
                <TableHead>模式</TableHead>
                <TableHead>UUID 状态</TableHead>
                <TableHead>转换状态</TableHead>
                <TableHead className="text-right">获取次数</TableHead>
                <TableHead>最后更新</TableHead>
                <TableHead className="min-w-48">信息</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {status.items.length ? (
                status.items.map((record: WatchRecord) => (
                  <TableRow key={record.uuid}>
                    <TableCell className="pl-6 font-mono text-xs font-medium">
                      {record.uuid}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs">
                      {record.mode_id
                        ? (MODE_LABELS[record.mode_id] ?? `Mode ${record.mode_id}`)
                        : "—"}
                    </TableCell>
                    <TableCell>
                      <StatusBadge state={record.uuid_state} labels={UUID_LABELS} />
                    </TableCell>
                    <TableCell>
                      <StatusBadge
                        state={record.conversion_state}
                        labels={CONVERSION_LABELS}
                      />
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {record.attempts}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                      {formatTime(record.updated_at)}
                    </TableCell>
                    <TableCell
                      className="max-w-64 truncate text-xs text-muted-foreground"
                      title={record.message ?? undefined}
                    >
                      {record.message ?? "—"}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={7} className="h-28 text-center">
                    <p className="text-sm font-medium">
                      {loading ? "正在读取 Watch 状态…" : "暂无 Watch UUID"}
                    </p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      majsoul2mjai watch 上报事件后会自动出现在这里
                    </p>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
