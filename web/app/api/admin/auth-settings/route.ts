import { getSessionToken, API_BASE } from "@/lib/session";

async function forward(request: Request) {
  const token = await getSessionToken();
  if (!token) return Response.json({ error: "unauthorized" }, { status: 401 });
  const response = await fetch(`${API_BASE}/api/v1/auth/settings`, {
    method: request.method,
    cache: "no-store",
    headers: {
      authorization: `Bearer ${process.env.MJAI_API_KEY ?? "change-me"}`,
      "x-mjai-user-session": token,
      "content-type": "application/json",
    },
    body: request.method === "GET" ? undefined : await request.text(),
  });
  return new Response(await response.text(), {
    status: response.status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export const GET = forward;
export const PUT = forward;
