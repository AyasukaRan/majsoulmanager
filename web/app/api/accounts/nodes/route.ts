import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

/**
 * Spreads the re-fetch pool over the nodes that can currently reach Mahjong
 * Soul. Forwarded like the pool itself: the machine key says the request came
 * from this console, the session says who is behind it, and the backend refuses
 * this without an administrator's.
 */
export async function POST() {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  try {
    const response = await fetch(new URL("/api/v1/accounts/nodes", baseUrl), {
      method: "POST",
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
