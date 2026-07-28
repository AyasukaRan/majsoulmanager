import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

type RouteContext = { params: Promise<{ path?: string[] }> };

async function forward(request: Request, context: RouteContext) {
  const { path = [] } = await context.params;
  const suffix = path.length ? `/${path.join("/")}` : "";
  return proxyToApi(request, `/api/v1/downloads${suffix}`, "导出服务暂不可用");
}

export const GET = forward;
export const POST = forward;
