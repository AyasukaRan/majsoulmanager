"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import { Layers3, LoaderCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export function AuthForm({ mode }: { mode: "login" | "register" | "verify" }) {
  const search = useSearchParams();
  const [registrationEnabled, setRegistrationEnabled] = useState(false);
  const [form, setForm] = useState({ name: "", email: "", password: "" });
  const [busy, setBusy] = useState(mode === "verify");
  const [message, setMessage] = useState("");

  useEffect(() => {
    const timer = window.setTimeout(async () => {
      if (mode === "register") {
        const response = await fetch("/api/auth/status");
        if (response.ok) {
          const status = await response.json() as { registration_enabled: boolean };
          setRegistrationEnabled(status.registration_enabled);
        }
      }
      if (mode === "verify") {
        const token = search.get("token");
        if (!token) {
          setMessage("验证链接缺少 token");
          setBusy(false);
          return;
        }
        const response = await fetch("/api/auth/verify-email", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ token }),
        });
        setMessage(response.ok ? "邮箱验证成功，现在可以登录。" : ((await response.json()).error ?? "验证失败"));
        setBusy(false);
      }
    }, 0);
    return () => window.clearTimeout(timer);
  }, [mode, search]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage("");
    try {
      const response = await fetch(`/api/auth/${mode}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(form),
      });
      if (response.ok) {
        if (mode === "login") {
          window.location.assign("/");
          return;
        }
        setMessage("注册成功。请检查邮箱并在 24 小时内完成验证。");
      } else {
        const value = await response.json() as { error?: string };
        setMessage(value.error ?? "请求失败");
      }
    } catch {
      setMessage("无法连接到服务，请稍后重试。");
    } finally {
      setBusy(false);
    }
  }

  const title = mode === "login" ? "登录管理台" : mode === "register" ? "创建账号" : "验证邮箱";
  return (
    <div className="grid min-h-svh place-items-center bg-muted/40 px-4">
      <div className="w-full max-w-sm">
        <Link href="/" className="mb-6 flex items-center justify-center gap-2 text-sm font-semibold">
          <span className="grid size-9 place-items-center rounded-lg bg-primary text-primary-foreground"><Layers3 className="size-5" /></span>
          mjai 管理台
        </Link>
        <Card className="shadow-none">
          <CardHeader><CardTitle>{title}</CardTitle><CardDescription>{mode === "login" ? "使用管理员或已验证账号继续" : mode === "register" ? "注册后必须完成邮箱验证" : "正在确认验证链接"}</CardDescription></CardHeader>
          <CardContent>
            {mode === "verify" ? (
              <div className="space-y-4 text-sm">{busy ? <p className="flex items-center gap-2"><LoaderCircle className="size-4 animate-spin" />正在验证…</p> : <p>{message}</p>}<Button className="w-full" render={<Link href="/login" />}>返回登录</Button></div>
            ) : (
              <form className="space-y-4" onSubmit={submit}>
                {mode === "register" && <label className="block space-y-1.5 text-xs font-medium">显示名称<Input required value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>}
                <label className="block space-y-1.5 text-xs font-medium">邮箱<Input required type="email" autoComplete="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} /></label>
                <label className="block space-y-1.5 text-xs font-medium">密码<Input required type="password" minLength={10} autoComplete={mode === "login" ? "current-password" : "new-password"} value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /></label>
                {message && <p className="rounded-md bg-muted p-3 text-xs">{message}</p>}
                <Button type="submit" className="w-full" disabled={busy || (mode === "register" && !registrationEnabled)}>{busy && <LoaderCircle className="size-4 animate-spin" />}{mode === "login" ? "登录" : registrationEnabled ? "注册并发送验证邮件" : "管理员已关闭注册"}</Button>
                <p className="text-center text-xs text-muted-foreground">{mode === "login" ? <>没有账号？ <Link className="text-primary hover:underline" href="/register">注册</Link></> : <>已有账号？ <Link className="text-primary hover:underline" href="/login">登录</Link></>}</p>
              </form>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
