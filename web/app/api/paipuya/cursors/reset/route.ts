import { forwardWithSession } from "@/lib/paipuya-forward";

export const dynamic = "force-dynamic";

export const POST = (request: Request) =>
  forwardWithSession(request, "cursors/reset");
