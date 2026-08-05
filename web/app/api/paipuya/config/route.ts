import { forwardWithSession } from "@/lib/paipuya-forward";

export const dynamic = "force-dynamic";

export const GET = (request: Request) => forwardWithSession(request, "config");
export const PUT = (request: Request) => forwardWithSession(request, "config");
