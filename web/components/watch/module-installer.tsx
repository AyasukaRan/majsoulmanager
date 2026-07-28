"use client";

import { useState } from "react";

import { jsonRequest, type InstalledWatchModule } from "@/lib/mjai-api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { RunAction } from "@/components/watch/busy-action";

export function WatchModuleInstaller({
  busy,
  run,
  onMessage,
  onInstalled,
}: {
  busy: boolean;
  run: RunAction;
  onMessage: (message: string) => void;
  /** Reloads the module lists, so the freshly installed one can be selected. */
  onInstalled: () => Promise<void>;
}) {
  const [manifest, setManifest] = useState<File | null>(null);
  const [artifact, setArtifact] = useState<File | null>(null);

  async function installModule() {
    if (!manifest || !artifact) {
      onMessage("请选择模块 manifest 和可执行文件");
      return;
    }
    await run("模块安装失败", async () => {
      const parsed = JSON.parse(await manifest.text()) as unknown;
      const dataUrl = await new Promise<string>((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result));
        reader.onerror = () => reject(reader.error);
        reader.readAsDataURL(artifact);
      });
      const artifactBase64 = dataUrl.slice(dataUrl.indexOf(",") + 1);
      const installed = await jsonRequest<InstalledWatchModule>(
        "/api/watch/modules",
        {
          method: "POST",
          body: JSON.stringify({
            manifest: parsed,
            artifact_base64: artifactBase64,
          }),
        },
      );
      // Reported before the reload rather than returned after it, so a reload
      // that cannot read the backend still gets to say that instead.
      onMessage(`模块 ${installed.name}@${installed.version} 已安装并通过健康检查`);
      setManifest(null);
      setArtifact(null);
      await onInstalled();
    });
  }

  return (
    <div className="rounded-lg border bg-muted/25 p-3">
      <p className="text-xs font-medium">安装热更新模块</p>
      <p className="mt-1 text-xs text-muted-foreground">
        上传 manifest.json 与对应单文件可执行程序；切换前会校验摘要和协议健康状态。
      </p>
      <div className="mt-3 grid gap-2 sm:grid-cols-[1fr_1fr_auto]">
        <Input
          type="file"
          accept="application/json,.json"
          aria-label="选择模块 manifest"
          onChange={(event) => setManifest(event.target.files?.[0] ?? null)}
        />
        <Input
          type="file"
          aria-label="选择模块可执行文件"
          onChange={(event) => setArtifact(event.target.files?.[0] ?? null)}
        />
        <Button
          variant="outline"
          onClick={() => void installModule()}
          disabled={busy}
        >
          安装
        </Button>
      </div>
    </div>
  );
}
