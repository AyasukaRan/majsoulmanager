import { cookies } from "next/headers";
import { API_BASE, SESSION_COOKIE } from "@/lib/session";

type Context = { params: Promise<{ action: string }> };

async function handler(request: Request, context: Context) {
  const { action } = await context.params;
  const allowed = new Set(["status", "login", "register", "verify-email", "logout", "me"]);
  if (!allowed.has(action)) {
    return Response.json({ error: "not found" }, { status: 404 });
  }
  const cookieStore = await cookies();
  const token = cookieStore.get(SESSION_COOKIE)?.value;
  const response = await fetch(`${API_BASE}/api/v1/auth/${action}`, {
    method: request.method,
    cache: "no-store",
    headers: {
      "content-type": "application/json",
      ...(token ? { "x-mjai-user-session": token } : {}),
    },
    body:
      request.method === "GET" || request.method === "HEAD"
        ? undefined
        : await request.text(),
  });
  const body = await response.text();
  if (action === "login" && response.ok) {
    const value = JSON.parse(body) as {
      session_token: string;
      expires_at: string;
      user: unknown;
    };
    cookieStore.set(SESSION_COOKIE, value.session_token, {
      httpOnly: true,
      sameSite: "strict",
      secure: new URL(request.url).protocol === "https:",
      path: "/",
      expires: new Date(value.expires_at),
    });
    return Response.json({ user: value.user });
  }
  if (action === "logout") {
    cookieStore.delete(SESSION_COOKIE);
  }
  return new Response(body || null, {
    status: response.status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export const GET = handler;
export const POST = handler;
