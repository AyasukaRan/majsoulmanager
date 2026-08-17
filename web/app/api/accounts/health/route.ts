import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

/**
 * Forwards what the last login with each account did.
 *
 * Behind the same session check as the pool itself, for the same reason: this
 * carries no password, but it does carry every account name, and that is what
 * the pool route is restricted for.
 */
async function forward() {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL("/api/v1/accounts/health", baseUrl);

  try {
    const response = await fetch(apiUrl, {
      method: "GET",
      cache: "no-store",
      headers: {
        authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
        "x-mjai-user-session": token,
      },
    });
    return new Response(await response.text(), {
      status: response.status,
      headers: {
        "content-type":
          response.headers.get("content-type") ??
          "application/json; charset=utf-8",
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

export const GET = forward;
