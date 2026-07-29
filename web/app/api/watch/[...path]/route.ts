import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

type RouteContext = {
  params: Promise<{ path: string[] }>;
};

async function forward(request: Request, context: RouteContext) {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const { path } = await context.params;
  const incoming = new URL(request.url);
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL(`/api/v1/watch/${path.join("/")}`, baseUrl);
  apiUrl.search = incoming.search;

  try {
    const headers: HeadersInit = {
      authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
      // The key says the request came from this console, which is true of every
      // request any member makes through it. Only the session says who, and the
      // backend refuses to start, stop or reconfigure collection without one
      // that belongs to an administrator.
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
    return Response.json(
      { error: "watch backend unavailable" },
      { status: 503 },
    );
  }
}

export const GET = forward;
export const POST = forward;
export const PUT = forward;
