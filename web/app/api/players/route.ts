import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  return proxyToApi(request, "/api/v1/players", "玩家检索暂不可用");
}
