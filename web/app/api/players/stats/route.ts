import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  return proxyToApi(request, "/api/v1/players/stats", "玩家战绩暂不可用");
}
