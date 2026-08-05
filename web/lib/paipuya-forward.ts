import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

/**
 * Forwards one 牌谱屋 call under the machine key *and* the caller's session.
 *
 * The session is what the backend reads to decide whether this person may
 * change the sync — the key only says the request came from this console, which
 * is true of every request any member makes through it.
 */
export async function forwardWithSession(request: Request, path: string) {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const incoming = new URL(request.url);
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL(`/api/v1/paipuya/${path}`, baseUrl);
  apiUrl.search = incoming.search;

  try {
    const headers: HeadersInit = {
      authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
      "x-mjai-user-session": token,
    };
    const contentType = request.headers.get("content-type");
    if (contentType) {
      headers["content-type"] = contentType;
    }
    const response = await fetch(apiUrl, {
      method: request.method,
      cache: "no-store",
      headers,
      body:
        request.method === "GET" || request.method === "HEAD"
          ? undefined
          : await request.text(),
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
    return Response.json({ error: "paipuya backend unavailable" }, { status: 503 });
  }
}
