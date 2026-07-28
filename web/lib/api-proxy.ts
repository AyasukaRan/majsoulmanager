import { API_BASE, getSessionUser } from "@/lib/session";

/**
 * Forwards one browser request to the Rust API under the machine key. The key
 * never leaves the server, so every data page has to come through a route
 * handler like this one rather than fetching the backend directly.
 *
 * The body is relayed as a stream instead of `await response.text()` because
 * these paths also serve a raw mjson and a multi-gigabyte export archive, and
 * buffering either one would put it in the Worker's heap. `content-disposition`
 * travels with it so the browser still gets the backend's filename.
 */
export async function proxyToApi(
  request: Request,
  path: string,
  unavailableMessage: string,
) {
  if (!(await getSessionUser())) {
    return Response.json({ error: "未登录或会话已过期" }, { status: 401 });
  }
  const apiUrl = new URL(path, API_BASE);
  apiUrl.search = new URL(request.url).search;

  try {
    const headers: HeadersInit = {
      authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
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
    const relayed = new Headers({
      "cache-control": "no-store",
      "content-type":
        response.headers.get("content-type") ??
        "application/json; charset=utf-8",
    });
    const disposition = response.headers.get("content-disposition");
    if (disposition) {
      relayed.set("content-disposition", disposition);
    }
    return new Response(response.body, {
      status: response.status,
      headers: relayed,
    });
  } catch {
    return Response.json({ error: unavailableMessage }, { status: 503 });
  }
}
