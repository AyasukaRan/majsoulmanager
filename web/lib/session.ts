import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export type SessionUser = {
  id: string;
  email: string;
  name: string;
  role: "admin" | "member";
  status: "active";
};

const SESSION_COOKIE = "mjai_session";
const API_BASE = process.env.MJAI_API_BASE_URL ?? "http://localhost:8000";

export async function getSessionToken() {
  return (await cookies()).get(SESSION_COOKIE)?.value ?? null;
}

export async function getSessionUser(): Promise<SessionUser | null> {
  const token = await getSessionToken();
  if (!token) return null;
  try {
    const response = await fetch(`${API_BASE}/api/v1/auth/me`, {
      cache: "no-store",
      headers: { "x-mjai-user-session": token },
    });
    return response.ok ? ((await response.json()) as SessionUser) : null;
  } catch {
    return null;
  }
}

export async function requireSessionUser() {
  const user = await getSessionUser();
  if (!user) redirect("/login");
  return user;
}

export { API_BASE, SESSION_COOKIE };
