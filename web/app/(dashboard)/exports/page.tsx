import { ExportJobs } from "@/components/export-jobs";

export default function ExportsPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 批量导出</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">批量导出</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          根据筛选快照异步生成大规模归档。
        </p>
      </div>
      <ExportJobs />
    </div>
  );
}
