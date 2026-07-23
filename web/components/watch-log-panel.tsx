"use client";

import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { ArrowDownToLine, Eraser, RefreshCw } from "lucide-react";

import type { WatchLogEntry, WatchLogLevel, WatchLogPage } from "@/lib/mjai-api";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

const MAX_ENTRIES = 800;
const POLL_LIMIT = 500;
const BOTTOM_THRESHOLD_PX = 40;
const FETCH_TIMEOUT_MS = 10_000;

const timeFormatter = new Intl.DateTimeFormat("zh-CN", {
  hour12: false,
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

function levelClass(level: WatchLogLevel) {
  switch (level) {
    case "info":
      return "text-sky-700 dark:text-sky-300";
    case "warn":
      return "text-amber-700 dark:text-amber-300";
    case "error":
      return "text-red-700 dark:text-red-300";
    default:
      return "text-zinc-500 dark:text-zinc-400";
  }
}

export function WatchLogPanel() {
  const [entries, setEntries] = useState<WatchLogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const cursorRef = useRef<number | null>(null);
  const bootIdRef = useRef<string | null>(null);
  const inFlightRef = useRef(false);
  const viewportRef = useRef<HTMLDivElement | null>(null);

  const fetchPage = useCallback(async (after: number | null) => {
    const query = new URLSearchParams({ limit: String(POLL_LIMIT) });
    if (after !== null) {
      query.set("after", String(after));
    }
    const response = await fetch(`/api/watch/logs?${query.toString()}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!response.ok) {
      throw new Error("watch logs unavailable");
    }
    return (await response.json()) as WatchLogPage;
  }, []);

  const refresh = useCallback(
    async (background = false) => {
      if (inFlightRef.current) {
        return;
      }
      inFlightRef.current = true;
      if (!background) {
        setLoading(true);
      }
      try {
        let page = await fetchPage(cursorRef.current);
        if (bootIdRef.current !== null && page.boot_id !== bootIdRef.current) {
          // 后端重启:seq 从头计数,旧游标会永远读到空页。清空重来,
          // 这正是启动日志出现的时刻。
          cursorRef.current = null;
          setEntries([]);
          page = await fetchPage(null);
        }
        bootIdRef.current = page.boot_id;
        if (page.next_cursor !== null) {
          cursorRef.current = page.next_cursor;
        }
        if (page.items.length) {
          setEntries((prev) => {
            const merged = [...prev, ...page.items];
            return merged.length > MAX_ENTRIES
              ? merged.slice(merged.length - MAX_ENTRIES)
              : merged;
          });
        }
        setError(null);
      } catch {
        setError("日志服务暂不可用,将自动重试");
      } finally {
        inFlightRef.current = false;
        if (!background) {
          setLoading(false);
        }
      }
    },
    [fetchPage],
  );

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(true), 3_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  useLayoutEffect(() => {
    if (!autoScroll) {
      return;
    }
    const viewport = viewportRef.current;
    if (viewport) {
      viewport.scrollTop = viewport.scrollHeight;
    }
  }, [entries, autoScroll]);

  const handleScroll = useCallback(() => {
    const viewport = viewportRef.current;
    if (!viewport) {
      return;
    }
    const distance =
      viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight;
    setAutoScroll(distance <= BOTTOM_THRESHOLD_PX);
  }, []);

  const scrollToBottom = useCallback(() => {
    setAutoScroll(true);
    const viewport = viewportRef.current;
    if (viewport) {
      viewport.scrollTop = viewport.scrollHeight;
    }
  }, []);

  return (
    <Card className="border-border/70 shadow-none">
      <CardHeader className="gap-4 border-b">
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
          <div>
            <div className="flex items-center gap-2">
              <CardTitle>服务日志</CardTitle>
              {error ? (
                <Badge
                  variant="outline"
                  className="border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300"
                >
                  {error}
                </Badge>
              ) : null}
            </div>
            <CardDescription className="mt-1">
              Watch 后台服务与协议模块的最近日志(内存环形缓冲,最多保留 500 条)
            </CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant={autoScroll ? "secondary" : "outline"}
              size="sm"
              className="gap-2"
              aria-pressed={autoScroll}
              onClick={() => (autoScroll ? setAutoScroll(false) : scrollToBottom())}
            >
              <ArrowDownToLine className="size-3.5" />
              自动滚动
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="gap-2"
              onClick={() => setEntries([])}
              disabled={!entries.length}
            >
              <Eraser className="size-3.5" />
              清空
            </Button>
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
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div
          ref={viewportRef}
          onScroll={handleScroll}
          tabIndex={0}
          role="log"
          aria-label="服务日志"
          className="h-72 overflow-y-auto px-6 py-3 font-mono text-xs"
        >
          {entries.length ? (
            entries.map((entry) => (
              <div key={entry.seq} className="flex items-start gap-2 py-0.5">
                <span
                  className="shrink-0 text-muted-foreground"
                  title={new Date(entry.timestamp).toLocaleString("zh-CN", {
                    hour12: false,
                  })}
                >
                  {timeFormatter.format(new Date(entry.timestamp))}
                </span>
                <Badge
                  variant="outline"
                  className="h-4 shrink-0 px-1.5 py-0 font-mono text-[10px] font-normal text-muted-foreground"
                >
                  {entry.source}
                </Badge>
                <span
                  className={cn(
                    "break-all whitespace-pre-wrap",
                    levelClass(entry.level),
                  )}
                >
                  {entry.message}
                </span>
              </div>
            ))
          ) : (
            <div className="flex h-full flex-col items-center justify-center text-center">
              <p className="text-sm font-medium text-muted-foreground">
                {loading ? "正在读取服务日志…" : "暂无日志"}
              </p>
              <p className="mt-1 text-xs text-muted-foreground">
                Watch 服务启动后日志会自动出现在这里
              </p>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
