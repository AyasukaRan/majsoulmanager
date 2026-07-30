import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  return proxyToApi(request, "/api/v1/stats/series", "统计服务暂不可用");
}
