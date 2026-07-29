"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  Archive,
  ChartLine,
  CircleGauge,
  Database,
  Layers3,
  LogOut,
  Radio,
  Settings,
  Users,
} from "lucide-react";
import type { SessionUser } from "@/lib/session";
import { Button } from "@/components/ui/button";

const nav = [
  { href: "/", label: "概览", icon: CircleGauge },
  { href: "/trends", label: "数据趋势", icon: ChartLine },
  { href: "/records", label: "牌谱索引", icon: Database },
  { href: "/watch", label: "Watch 服务", icon: Radio },
  { href: "/exports", label: "批量导出", icon: Archive },
  // Last, beside 用户管理: the two entries that change how the deployment runs
  // rather than report on it, and the two the page itself refuses to a member.
  { href: "/settings", label: "设置", icon: Settings, admin: true },
  { href: "/users", label: "用户管理", icon: Users, admin: true },
];

function GitHubIcon({ className }: { className?: string }) {
  return (
    <svg
      aria-hidden="true"
      className={className}
      viewBox="0 0 24 24"
      fill="currentColor"
    >
      <path d="M12 .7a11.5 11.5 0 0 0-3.64 22.42c.58.1.79-.25.79-.56v-2.23c-3.22.7-3.9-1.37-3.9-1.37-.53-1.34-1.29-1.7-1.29-1.7-1.05-.72.08-.7.08-.7 1.16.08 1.78 1.2 1.78 1.2 1.04 1.77 2.72 1.26 3.38.96.1-.75.4-1.26.74-1.55-2.57-.3-5.28-1.29-5.28-5.69 0-1.26.45-2.29 1.19-3.1-.12-.3-.52-1.47.11-3.06 0 0 .97-.31 3.16 1.18A10.98 10.98 0 0 1 12 6.12c.98 0 1.96.13 2.88.38 2.2-1.49 3.16-1.18 3.16-1.18.63 1.59.23 2.77.11 3.06.74.81 1.19 1.84 1.19 3.1 0 4.42-2.72 5.39-5.3 5.68.42.36.79 1.07.79 2.16v3.24c0 .31.2.67.8.56A11.5 11.5 0 0 0 12 .7Z" />
    </svg>
  );
}

export function AppShell({
  user,
  children,
}: {
  user: SessionUser;
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const router = useRouter();
  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  }
  return (
    <div className="min-h-svh bg-muted/35">
      <header className="sticky top-0 z-30 border-b bg-background/95 backdrop-blur">
        <div className="mx-auto flex h-16 max-w-[1600px] items-center gap-5 px-4 md:px-6">
          <Link href="/" className="flex min-w-fit items-center gap-2.5">
            <span className="grid size-8 place-items-center rounded-lg bg-primary text-primary-foreground">
              <Layers3 className="size-[18px]" />
            </span>
            <span>
              <span className="block text-sm font-semibold leading-none">mjai 管理台</span>
              <span className="mt-1 block font-mono text-[10px] text-muted-foreground">DATA CONTROL PLANE</span>
            </span>
          </Link>
          <nav className="hidden items-center gap-1 lg:flex">
            {nav
              .filter((item) => !item.admin || user.role === "admin")
              .map(({ href, label, icon: Icon }) => (
                <Button
                  key={href}
                  variant={pathname === href ? "secondary" : "ghost"}
                  size="sm"
                  render={<Link href={href} />}
                >
                  <Icon className="size-4" />
                  {label}
                </Button>
              ))}
          </nav>
          <div className="ml-auto flex items-center gap-2">
            <Button
              variant="ghost"
              size="icon"
              aria-label="打开 GitHub 仓库"
              render={
                <a
                  href="https://github.com/AyasukaRan/majsoulmanager"
                  target="_blank"
                  rel="noreferrer"
                />
              }
            >
              <GitHubIcon className="size-4" />
            </Button>
            <div className="hidden text-right sm:block">
              <p className="text-xs font-medium">{user.name}</p>
              <p className="text-[10px] text-muted-foreground">{user.email}</p>
            </div>
            <Button variant="outline" size="icon" aria-label="退出登录" onClick={() => void logout()}>
              <LogOut className="size-4" />
            </Button>
          </div>
        </div>
      </header>
      <nav className="border-b bg-background px-3 py-2 lg:hidden">
        <div className="mx-auto flex max-w-[1600px] gap-1 overflow-x-auto">
          {nav
            .filter((item) => !item.admin || user.role === "admin")
            .map(({ href, label, icon: Icon }) => (
              <Button
                key={href}
                variant={pathname === href ? "secondary" : "ghost"}
                size="sm"
                className="shrink-0"
                render={<Link href={href} />}
              >
                <Icon className="size-4" />
                {label}
              </Button>
            ))}
        </div>
      </nav>
      <main className="mx-auto max-w-[1600px] px-4 py-6 md:px-6 md:py-8">{children}</main>
    </div>
  );
}
