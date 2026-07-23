import { AppShell } from "@/components/app-shell";
import { requireSessionUser } from "@/lib/session";

export default async function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const user = await requireSessionUser();
  return <AppShell user={user}>{children}</AppShell>;
}
