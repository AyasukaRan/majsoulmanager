import { PlayerStats } from "@/components/player-stats";

export default function PlayersPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs text-muted-foreground">控制台 / 玩家战绩</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">玩家战绩</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          每一局的每个座位都被计过分，这里把它们按玩家、场次和时间加起来。
        </p>
      </div>
      <PlayerStats />
    </div>
  );
}
