import { proxyToApi } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  return proxyToApi(request, "/api/v1/paipuya/gap", "牌谱屋对照暂不可用");
}
