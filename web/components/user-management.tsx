"use client";

import { useCallback, useEffect, useState } from "react";
import { ShieldCheck, UserPlus } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

type User = { id: string; email: string; name: string; role: "admin" | "member"; status: "pending_verification" | "active" | "disabled"; created_at: string; last_login_at: string | null };
type Settings = { registration_enabled: boolean; email_delivery_configured: boolean };

export function UserManagement() {
  const [users, setUsers] = useState<User[]>([]);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [form, setForm] = useState({ name: "", email: "", password: "", role: "member" });
  const [message, setMessage] = useState("");

  const refresh = useCallback(async () => {
    const [usersResponse, settingsResponse] = await Promise.all([fetch("/api/users"), fetch("/api/admin/auth-settings")]);
    if (usersResponse.ok) setUsers(await usersResponse.json() as User[]);
    if (settingsResponse.ok) setSettings(await settingsResponse.json() as Settings);
  }, []);
  useEffect(() => { const timer = window.setTimeout(() => void refresh(), 0); return () => window.clearTimeout(timer); }, [refresh]);

  async function toggleRegistration(value: boolean) {
    const response = await fetch("/api/admin/auth-settings", { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ registration_enabled: value }) });
    const result = await response.json() as Settings & { error?: string };
    if (response.ok) setSettings(result); else setMessage(result.error ?? "更新失败");
  }
  async function createUser(event: React.FormEvent) {
    event.preventDefault();
    const response = await fetch("/api/users", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(form) });
    if (response.ok) { setForm({ name: "", email: "", password: "", role: "member" }); await refresh(); setMessage("用户已创建"); }
    else setMessage(((await response.json()) as { error?: string }).error ?? "创建失败");
  }
  async function updateUser(id: string, patch: Partial<User>) {
    const response = await fetch(`/api/users/${id}`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(patch) });
    if (response.ok) await refresh(); else setMessage(((await response.json()) as { error?: string }).error ?? "更新失败");
  }

  return (
    <div className="space-y-6">
      <Card className="shadow-none"><CardHeader><CardTitle className="flex items-center gap-2"><ShieldCheck className="size-4" />注册策略</CardTitle><CardDescription>只有邮件投递 API 配置可用时才能开放注册；新用户必须验证邮箱。</CardDescription></CardHeader><CardContent><label className="flex items-center gap-3 text-sm"><Checkbox checked={settings?.registration_enabled ?? false} disabled={!settings?.email_delivery_configured} onCheckedChange={(checked) => void toggleRegistration(checked)} />允许公开注册</label><p className="mt-2 text-xs text-muted-foreground">邮件服务：{settings?.email_delivery_configured ? "已配置" : "未配置，注册保持关闭"}</p></CardContent></Card>
      <Card className="shadow-none"><CardHeader><CardTitle className="flex items-center gap-2"><UserPlus className="size-4" />创建用户</CardTitle><CardDescription>管理员创建的账号默认已验证，可立即登录。</CardDescription></CardHeader><CardContent><form className="grid gap-3 md:grid-cols-4" onSubmit={createUser}><Input required placeholder="名称" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /><Input required type="email" placeholder="邮箱" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} /><Input required type="password" minLength={10} placeholder="初始密码" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /><div className="flex gap-2"><select className="h-9 flex-1 rounded-lg border bg-background px-2 text-sm" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}><option value="member">成员</option><option value="admin">管理员</option></select><Button type="submit">创建</Button></div></form>{message && <p className="mt-3 text-xs text-muted-foreground">{message}</p>}</CardContent></Card>
      <Card className="shadow-none"><CardHeader><CardTitle>用户列表</CardTitle><CardDescription>{users.length} 个账号</CardDescription></CardHeader><CardContent className="p-0"><Table><TableHeader><TableRow><TableHead className="pl-6">用户</TableHead><TableHead>角色</TableHead><TableHead>状态</TableHead><TableHead>最近登录</TableHead><TableHead className="text-right pr-6">操作</TableHead></TableRow></TableHeader><TableBody>{users.map((user) => <TableRow key={user.id}><TableCell className="pl-6"><p className="font-medium">{user.name}</p><p className="text-xs text-muted-foreground">{user.email}</p></TableCell><TableCell><select className="h-8 rounded-md border bg-background px-2 text-xs" value={user.role} onChange={(e) => void updateUser(user.id, { role: e.target.value as User["role"] })}><option value="member">成员</option><option value="admin">管理员</option></select></TableCell><TableCell><Badge variant="outline">{user.status === "active" ? "正常" : user.status === "disabled" ? "已禁用" : "待验证"}</Badge></TableCell><TableCell className="text-xs text-muted-foreground">{user.last_login_at ? new Date(user.last_login_at).toLocaleString("zh-CN") : "从未"}</TableCell><TableCell className="pr-6 text-right"><Button variant="outline" size="sm" onClick={() => void updateUser(user.id, { status: user.status === "disabled" ? "active" : "disabled" })}>{user.status === "disabled" ? "启用" : "禁用"}</Button></TableCell></TableRow>)}</TableBody></Table></CardContent></Card>
    </div>
  );
}
