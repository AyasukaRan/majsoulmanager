import { redirect } from "next/navigation";
import { UserManagement } from "@/components/user-management";
import { requireSessionUser } from "@/lib/session";

export default async function UsersPage() {
  const user = await requireSessionUser();
  if (user.role !== "admin") redirect("/");
  return <div className="space-y-6"><div><p className="text-xs text-muted-foreground">控制台 / 用户管理</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">用户与注册</h1><p className="mt-1 text-sm text-muted-foreground">管理角色、账号状态、公开注册和邮箱验证能力。</p></div><UserManagement /></div>;
}
