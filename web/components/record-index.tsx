"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Download, RefreshCw, Search } from "lucide-react";

import {
  buildRecordSearch,
  jsonRequest,
  type MjaiRecord,
  type RecordPage,
} from "@/lib/mjai-api";
import { dayEnd, dayStart, formatDateTime } from "@/lib/utils";
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

const PAGE_SIZE = 50;

type Filters = {
  source: string;
  player: string;
  received_from: string;
  received_to: string;
};

const EMPTY_FILTERS: Filters = {
  source: "",
  player: "",
  received_from: "",
  received_to: "",
};

/**
 * A bare `<input type="date">` value is a calendar day, but the backend filters
 * on an instant. `dayStart`/`dayEnd` turn it into the operator's own day rather
 * than UTC's, which is the day the table is already rendering timestamps in.
 */
function searchUrl(filters: Filters, cursor: string | null) {
  return buildRecordSearch({
    source: filters.source.trim(),
    player: filters.player.trim(),
    received_from: dayStart(filters.received_from),
    received_to: dayEnd(filters.received_to),
    limit: PAGE_SIZE,
    cursor: cursor ?? "",
  });
}

export function RecordIndex() {
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS);
  const [items, setItems] = useState<MjaiRecord[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const inFlightRef = useRef(false);
  // Read inside the loader so "加载更多" keeps paging the filters that produced
  // the rows on screen, even if the operator has already retyped the inputs.
  const activeRef = useRef<Filters>(EMPTY_FILTERS);

  const load = useCallback(
    async (next: Filters | null) => {
      if (inFlightRef.current) {
        return;
      }
      inFlightRef.current = true;
      const reset = next !== null;
      if (reset) {
        activeRef.current = next;
      }
      setLoading(true);
      try {
        const page = await jsonRequest<RecordPage>(
          searchUrl(activeRef.current, reset ? null : cursor),
        );
        setItems((previous) =>
          reset ? page.items : [...previous, ...page.items],
        );
        setCursor(page.next_cursor);
        setError(null);
      } catch (caught) {
        // The 90-day window rejection arrives here as the backend's own
        // sentence; showing an empty table instead would read as "no data".
        setError(caught instanceof Error ? caught.message : "读取牌谱索引失败");
        if (reset) {
          setItems([]);
          setCursor(null);
        }
      } finally {
        inFlightRef.current = false;
        setLoading(false);
      }
    },
    [cursor],
  );

  useEffect(() => {
    const initial = window.setTimeout(() => void load(EMPTY_FILTERS), 0);
    return () => window.clearTimeout(initial);
    // Deliberately runs once: this table refreshes on demand, not on a timer.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <Card className="shadow-none">
      <CardHeader className="border-b">
        <CardTitle>索引记录</CardTitle>
        <CardDescription>
          按来源、玩家和入库时间筛选；未指定时间时后端只返回最近 90
          天，跨度超过 90 天会被拒绝。
        </CardDescription>
        <form
          className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-5"
          onSubmit={(event) => {
            event.preventDefault();
            void load(filters);
          }}
        >
          <label className="space-y-1.5 text-xs font-medium">
            来源
            <Input
              value={filters.source}
              placeholder="majsoul-watch"
              onChange={(event) =>
                setFilters({ ...filters, source: event.target.value })
              }
            />
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            玩家
            <Input
              value={filters.player}
              placeholder="玩家昵称"
              onChange={(event) =>
                setFilters({ ...filters, player: event.target.value })
              }
            />
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            入库起始
            <Input
              type="date"
              value={filters.received_from}
              onChange={(event) =>
                setFilters({ ...filters, received_from: event.target.value })
              }
            />
          </label>
          <label className="space-y-1.5 text-xs font-medium">
            入库截止
            <Input
              type="date"
              value={filters.received_to}
              onChange={(event) =>
                setFilters({ ...filters, received_to: event.target.value })
              }
            />
          </label>
          <div className="flex items-end gap-2">
            <Button type="submit" disabled={loading}>
              <Search className="size-4" />
              查询
            </Button>
            <Button
              type="button"
              variant="outline"
              disabled={loading}
              onClick={() => {
                setFilters(EMPTY_FILTERS);
                void load(EMPTY_FILTERS);
              }}
            >
              <RefreshCw
                className={`size-3.5 ${loading ? "animate-spin" : ""}`}
              />
              重置
            </Button>
          </div>
        </form>
        {error ? (
          <p className="mt-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300">
            查询失败：{error}
          </p>
        ) : null}
      </CardHeader>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="pl-6">记录 ID</TableHead>
              <TableHead>来源</TableHead>
              <TableHead>玩家</TableHead>
              <TableHead>规则</TableHead>
              <TableHead className="text-right">事件</TableHead>
              <TableHead>对局时间</TableHead>
              <TableHead>入库时间</TableHead>
              <TableHead className="pr-6 text-right">原始牌谱</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.length ? (
              items.map((record) => (
                <TableRow key={record.id}>
                  <TableCell className="pl-6 font-mono text-xs font-medium">
                    {record.id}
                  </TableCell>
                  <TableCell>
                    <Badge variant="secondary">{record.source}</Badge>
                  </TableCell>
                  <TableCell className="max-w-72 truncate text-xs">
                    {record.players.join(" / ")}
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs">
                    {record.rule ?? "—"}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {record.event_count.toLocaleString("zh-CN")}
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                    {formatDateTime(record.played_at)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                    {formatDateTime(record.received_at)}
                  </TableCell>
                  <TableCell className="pr-6 text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      render={
                        <a
                          href={`/api/records/${record.id}/raw`}
                          download={`${record.id}.mjson`}
                        />
                      }
                    >
                      <Download className="size-3.5" />
                      下载
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={8} className="h-28 text-center">
                  <p className="text-sm font-medium">
                    {loading
                      ? "正在读取牌谱索引…"
                      : error
                        ? "无法读取牌谱索引"
                        : "没有符合条件的牌谱"}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {error
                      ? "这是后端返回的错误，不代表索引为空"
                      : "调整筛选条件，或确认采集端是否仍在入库"}
                  </p>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </CardContent>
      {items.length ? (
        <div className="flex items-center justify-between border-t px-6 py-3 text-xs text-muted-foreground">
          <span>已加载 {items.length.toLocaleString("zh-CN")} 条</span>
          <Button
            variant="outline"
            size="sm"
            disabled={loading || cursor === null}
            onClick={() => void load(null)}
          >
            {cursor === null ? "已到末尾" : loading ? "加载中…" : "加载更多"}
          </Button>
        </div>
      ) : null}
    </Card>
  );
}
