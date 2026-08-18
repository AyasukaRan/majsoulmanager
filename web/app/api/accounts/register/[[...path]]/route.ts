import { getSessionToken, getSessionUser } from "@/lib/session";

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

/**
 * Which sub-paths of the registrar this console will forward.
 *
 * A list rather than a pass-through: the segment is joined onto a URL that
 * already carries the machine key and an administrator's session, so anything
 * accepted here is reachable with both. `""` is the run itself.
 */
const PATHS = new Set(["", "status", "stop"]);

/**
 * Forwards a registration request.
 *
 * Same shape as `/api/accounts`, and behind the same session check for a
 * stronger reason: this one creates credentials and spends the operator's
 * mailboxes. The body carries mailbox credentials on the way in and never on
 * the way back — the backend answers with addresses only.
 */
async function forward(
  request: Request,
  context: { params: Promise<{ path?: string[] }> },
) {
  const token = await getSessionToken();
  if (!token || !(await getSessionUser())) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const { path } = await context.params;
  const suffix = (path ?? []).join("/");
  if (!PATHS.has(suffix)) {
    return Response.json({ error: "not found" }, { status: 404 });
  }
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL(
    `/api/v1/accounts/register${suffix ? `/${suffix}` : ""}`,
    baseUrl,
  );

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
export const POST = forward;
