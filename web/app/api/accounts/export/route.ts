import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

/**
 * The re-fetch pool as `username,password` lines.
 *
 * The one response in this console that carries passwords in full, and it is
 * deliberate: an operator who registered these through the console has them in
 * exactly one place. Streamed straight through — no parsing, no logging, no
 * caching — and the backend refuses it without an administrator's session.
 */
export async function GET() {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  try {
    const response = await fetch(new URL("/api/v1/accounts/export", baseUrl), {
      cache: "no-store",
      headers: {
        authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
        "x-mjai-user-session": token,
      },
    });
    if (!response.ok) {
      return new Response(await response.text(), { status: response.status });
    }
    return new Response(response.body, {
      status: 200,
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "content-disposition":
          response.headers.get("content-disposition") ??
          'attachment; filename="majsoul-refetch-accounts.txt"',
        "cache-control": "no-store",
      },
    });
  } catch {
    return Response.json(
      { error: "accounts backend unavailable" },
      { status: 503 },
    );
  }
}
