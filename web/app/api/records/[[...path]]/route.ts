import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

type RouteContext = { params: Promise<{ path?: string[] }> };

/**
 * Read-only on purpose. The same backend path accepts POST for ingestion, and
 * nothing in the console should be able to write records through an operator's
 * session, so only GET is exported.
 */
export async function GET(request: Request, context: RouteContext) {
  const { path = [] } = await context.params;
  const suffix = path.length ? `/${path.join("/")}` : "";
  return proxyToApi(request, `/api/v1/records${suffix}`, "牌谱索引服务暂不可用");
}
