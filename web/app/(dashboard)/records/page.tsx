import { RecordIndex } from "@/components/record-index";

export default function RecordsPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 牌谱索引</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">牌谱索引</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          筛选、定位并读取任意单条 mjson。
        </p>
      </div>
      <RecordIndex />
    </div>
  );
}
