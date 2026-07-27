"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Archive, Download, RefreshCw } from "lucide-react";

import {
  jsonRequest,
  type DownloadFormat,
  type DownloadJob,
  type DownloadJobPage,
  type JobState,
} from "@/lib/mjai-api";
import { dayEnd, dayStart, formatDateTime, selectClass } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const STATE_LABELS: Record<JobState, string> = {
  queued: "排队中",
  running: "导出中",
  completed: "已完成",
  failed: "已失败",
};

function stateClass(state: JobState) {
  switch (state) {
    case "running":
      return "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300";
    case "completed":
      return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-300";
    case "failed":
      return "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300";
    default:
      return "border-border bg-muted text-muted-foreground";
  }
}

type Draft = {
  source: string;
  received_from: string;
  received_to: string;
  format: DownloadFormat;
};

const EMPTY_DRAFT: Draft = {
  source: "",
  received_from: "",
  received_to: "",
  format: "tar.gz",
};

export function ExportJobs() {
  const [jobs, setJobs] = useState<DownloadJob[]>([]);
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const inFlightRef = useRef(false);

  const refresh = useCallback(async () => {
    if (inFlightRef.current) {
      return;
    }
    inFlightRef.current = true;
    try {
      const page = await jsonRequest<DownloadJobPage>("/api/downloads");
      setJobs(page.items);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "读取导出任务失败");
    } finally {
      inFlightRef.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(), 5_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  async function createJob() {
    setBusy(true);
    setMessage(null);
    try {
      // Only non-empty fields go in: the job stores this filter verbatim, and an
      // empty string would narrow the export to records with an empty source.
      const filter = Object.fromEntries(
        Object.entries({
          source: draft.source.trim(),
          received_from: dayStart(draft.received_from),
          received_to: dayEnd(draft.received_to),
        }).filter(([, value]) => value !== ""),
      );
      const job = await jsonRequest<DownloadJob>("/api/downloads", {
        method: "POST",
        body: JSON.stringify({ filter, format: draft.format }),
      });
      setJobs((previous) => [job, ...previous]);
      setDraft(EMPTY_DRAFT);
      setMessage(`导出任务 ${job.id} 已提交`);
    } catch (caught) {
      setMessage(caught instanceof Error ? caught.message : "提交导出失败");
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <Card className="shadow-none">
        <CardHeader className="border-b">
          <CardTitle className="flex items-center gap-2">
            <Archive className="size-4" />
            新建导出
          </CardTitle>
          <CardDescription>
            导出按筛选快照异步执行；时间范围与索引页一致，跨度不能超过 90 天。
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-5">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            <label className="space-y-1.5 text-xs font-medium">
              来源
              <Input
                value={draft.source}
                placeholder="留空导出全部来源"
                onChange={(event) =>
                  setDraft({ ...draft, source: event.target.value })
                }
              />
            </label>
            <label className="space-y-1.5 text-xs font-medium">
              入库起始
              <Input
                type="date"
                value={draft.received_from}
                onChange={(event) =>
                  setDraft({ ...draft, received_from: event.target.value })
                }
              />
            </label>
            <label className="space-y-1.5 text-xs font-medium">
              入库截止
              <Input
                type="date"
                value={draft.received_to}
                onChange={(event) =>
                  setDraft({ ...draft, received_to: event.target.value })
                }
              />
            </label>
            <label className="space-y-1.5 text-xs font-medium">
              格式
              <select
                className={selectClass}
                value={draft.format}
                onChange={(event) =>
                  setDraft({
                    ...draft,
                    format: event.target.value as DownloadFormat,
                  })
                }
              >
                <option value="tar.gz">牌谱归档(tar.gz)</option>
                <option value="manifest.jsonl">索引清单(manifest.jsonl)</option>
              </select>
            </label>
            <div className="flex items-end">
              <Button disabled={busy} onClick={() => void createJob()}>
                <Archive className="size-4" />
                提交导出
              </Button>
            </div>
          </div>
          {message ? (
            <p className="mt-3 text-xs text-muted-foreground">{message}</p>
          ) : null}
        </CardContent>
      </Card>

      <Card className="shadow-none">
        <CardHeader className="border-b">
          <div className="flex items-center justify-between gap-3">
            <div>
              <CardTitle>导出任务</CardTitle>
              <CardDescription className="mt-1">
                运行中的任务没有总量可比，只显示当前已写入的条数。
              </CardDescription>
            </div>
            <Button
              variant="outline"
              size="sm"
              className="gap-2"
              disabled={loading}
              onClick={() => void refresh()}
            >
              <RefreshCw
                className={`size-3.5 ${loading ? "animate-spin" : ""}`}
              />
              刷新
            </Button>
          </div>
          {error ? (
            <p className="mt-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
              无法读取导出任务：{error}
            </p>
          ) : null}
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">任务 ID</TableHead>
                <TableHead>状态</TableHead>
                <TableHead>创建时间</TableHead>
                <TableHead className="text-right">已导出条数</TableHead>
                <TableHead className="min-w-48">信息</TableHead>
                <TableHead className="pr-6 text-right">归档</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.length ? (
                jobs.map((job) => (
                  <TableRow key={job.id}>
                    <TableCell className="pl-6 font-mono text-xs font-medium">
                      {job.id}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className={stateClass(job.state)}>
                        {STATE_LABELS[job.state]}
                      </Badge>
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                      {formatDateTime(job.created_at)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {job.state === "queued"
                        ? "—"
                        : job.record_count.toLocaleString("zh-CN")}
                    </TableCell>
                    <TableCell
                      className="max-w-64 truncate text-xs text-muted-foreground"
                      title={job.error ?? undefined}
                    >
                      {job.error ??
                        (job.state === "completed" ? "导出完成" : "—")}
                    </TableCell>
                    <TableCell className="pr-6 text-right">
                      {job.state === "completed" ? (
                        <Button
                          variant="outline"
                          size="sm"
                          render={<a href={`/api/downloads/${job.id}/file`} />}
                        >
                          <Download className="size-3.5" />
                          下载
                        </Button>
                      ) : (
                        <span className="text-xs text-muted-foreground">
                          尚未生成
                        </span>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={6} className="h-28 text-center">
                    <p className="text-sm font-medium">
                      {loading
                        ? "正在读取导出任务…"
                        : error
                          ? "无法读取导出任务"
                          : "暂无导出任务"}
                    </p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {error
                        ? "这是后端返回的错误，不代表没有任务"
                        : "提交一次导出后任务会出现在这里"}
                    </p>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </>
  );
}
