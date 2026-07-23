const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const incoming = new URL(request.url);
  const baseUrl = process.env.MJAI_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  const apiUrl = new URL("/api/v1/watch/status", baseUrl);
  apiUrl.search = incoming.search;

  try {
    const response = await fetch(apiUrl, {
      cache: "no-store",
      headers: {
        authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
      },
    });
    const body = await response.text();
    return new Response(body, {
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
      { error: "watch status service unavailable" },
      { status: 503 },
    );
  }
}
