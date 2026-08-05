import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

/**
 * Forwards the account pool, the same way `/api/refetch` does and for the same
 * reason: the machine key says the request came from this console, and only the
 * session says who is behind it. The backend refuses a write without an
 * administrator's session, so the token has to travel with the request.
 *
 * Nothing here redacts anything — the backend already answers `***` for every
 * password, which is why a plain forward is safe.
 */
async function forward(request: Request) {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL("/api/v1/accounts", baseUrl);

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
      body: request.method === "GET" ? undefined : await request.text(),
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
export const PUT = forward;
