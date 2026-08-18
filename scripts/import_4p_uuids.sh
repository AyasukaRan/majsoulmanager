#!/usr/bin/env bash
# 把本地枚举出来的 4 麻历史 uuid 导入 mjai.paipuya_games。
#
# 数据来自 majsoul2mjai 的 rust-state, 分在两个文件里, 都以 (mode, short_uuid) 为键:
#   resolved.tsv   mode \t short_uuid \t full_uuid            ← 雀魂要的完整 uuid
#   short.txt      mode \t short_uuid \t start_time \t 四个 account_id
# 两边都缺对方的一半, 所以必须连接。本地排序两个 10G 文件要一个多小时, 而 ClickHouse
# 做两亿行的 join 是几十秒的事 —— 所以两张 Log 引擎临时表各自灌进去, 一条
# INSERT ... SELECT 收尾。
#
# 落到 mjai.paipuya_games 而不是新建表, 是因为要的正是那张表的消费方: refetch 的
# 牌谱屋走查 (one_paipuya_pass) 按 (started_at, uuid) 分页, 先用 claimed_games 一次
# 索引查询滤掉本地已有的, 再去雀魂取剩下的。换张表就得把那条链路重写一遍。
#
# ⚠ players/scores 留空 —— 这两个文件里没有昵称。后果是控制台那张"缺口对比"卡片
#   (paipuya_gap) 对这批行永远算成缺失: 它按 (开始时间, 排序后的昵称集合) 匹配, 空集
#   配不上任何一局。补抓走查本身不受影响, 它按 uuid 判重。
#
# ⚠ 1.91 亿局按补抓池现实吞吐是以年计的。导入本身很便宜 (几个 GB), 但它是个工作清单
#   不是计划表 —— 配合 refetch 的 paipuya_from 圈定真正要的区间。
#
# ── 两种跑法 ────────────────────────────────────────────────────────────────
# 【本地】数据文件和 ClickHouse 在同一台机器上 —— 这是搬到局域网之后的常态, 因为那两个
#         TSV 就躺在跑 ClickHouse 那台的盘上。什么都不用传, 直接喂给 clickhouse-client:
#
#           ./import_4p_uuids.sh /path/to/rust-state
#
# 【远程】数据在这台、ClickHouse 在别处。压缩后走 ssh —— 21GB 原始文本, 压完约 4GB,
#         取决于上行能跑多久:
#
#           SSH_HOST=user@172.16.0.110 ./import_4p_uuids.sh /path/to/rust-state
#
#   DRY_RUN=1 ./import_4p_uuids.sh <dir>      # 只做本地抽取和计数, 不碰服务器
set -euo pipefail

SRC="${1:?用法: $0 <rust-state 目录> (里面是 *.resolved.tsv 和 *.short.txt)}"
PREFIX="${PREFIX:-all-4p-east-south}"
DB="${DB:-mjai}"
DRY_RUN="${DRY_RUN:-0}"
# 空 = 本地模式。给了就走 ssh。
SSH_HOST="${SSH_HOST:-}"
SSH_PORT="${SSH_PORT:-22}"
# ClickHouse 跑在容器里时的容器名。留空则用 PATH 里的 clickhouse-client。
CH_CONTAINER="${CH_CONTAINER:-}"

RESOLVED="$SRC/$PREFIX.resolved.tsv"
SHORT="$SRC/$PREFIX.short.txt"
[ -r "$RESOLVED" ] || { echo "读不到 $RESOLVED" >&2; exit 1; }
[ -r "$SHORT" ]    || { echo "读不到 $SHORT" >&2; exit 1; }

# 容器名没给就找一个。ClickHouse 在这套部署里是跑在 compose 里的, 名字随项目目录变,
# 所以猜不如问 docker。
if [ "$DRY_RUN" != 1 ] && [ -z "$SSH_HOST" ] && [ -z "$CH_CONTAINER" ] \
   && ! command -v clickhouse-client >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1; then
    CH_CONTAINER="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i clickhouse | head -1 || true)"
  fi
  [ -n "$CH_CONTAINER" ] || {
    echo "找不到 clickhouse-client, 也没在 docker 里找到 clickhouse 容器。" >&2
    echo "用 CH_CONTAINER=<容器名> 指定, 或用 SSH_HOST=<主机> 走远程。" >&2
    exit 1
  }
  echo "(用 docker 容器 $CH_CONTAINER)"
fi

# 一条查询。三种通路: dry-run 打印 / 本地(可能过 docker) / ssh。
ch() {
  if [ "$DRY_RUN" = 1 ]; then echo "  [dry-run] clickhouse-client --query \"$1\""; return; fi
  if [ -n "$SSH_HOST" ]; then
    ssh -p "$SSH_PORT" "$SSH_HOST" "clickhouse-client --query \"$1\""
  elif [ -n "$CH_CONTAINER" ]; then
    docker exec -i "$CH_CONTAINER" clickhouse-client --query "$1"
  else
    clickhouse-client --query "$1"
  fi
}

echo "== 1/4 建临时表 =="
# Log 引擎: 只写一次、只读一次、用完就删, 不需要 MergeTree 的任何东西。
ch "CREATE TABLE IF NOT EXISTS $DB.import_4p_resolved (mode Int32, short_uuid String, uuid String) ENGINE = Log"
ch "CREATE TABLE IF NOT EXISTS $DB.import_4p_short (mode Int32, short_uuid String, start_time Int64) ENGINE = Log"
ch "TRUNCATE TABLE $DB.import_4p_resolved"
ch "TRUNCATE TABLE $DB.import_4p_short"

push() {
  local file="$1" table="$2" cut_cols="$3"
  echo "   -> $table  ($(du -h "$file" | cut -f1) 原始)"
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [dry-run] 前 3 行会长这样:"
    # head 提前退出会给 cut 一个 SIGPIPE, 配 pipefail 就是整脚本挂掉 —— 所以先截行再切列
    head -3 "$file" | cut -f"$cut_cols" | sed 's/^/      /'
    return
  fi
  local query="INSERT INTO $DB.$table FORMAT TSV"
  if [ -n "$SSH_HOST" ]; then
    # 只有跨机器才值得压。uuid 和时间戳都是高度可压的文本, 实测 4:1 上下。
    cut -f"$cut_cols" "$file" \
      | zstd -3 -T0 -c \
      | ssh -p "$SSH_PORT" "$SSH_HOST" "zstd -d -c | clickhouse-client --query '$query'"
  elif [ -n "$CH_CONTAINER" ]; then
    cut -f"$cut_cols" "$file" | docker exec -i "$CH_CONTAINER" clickhouse-client --query "$query"
  else
    cut -f"$cut_cols" "$file" | clickhouse-client --query "$query"
  fi
}

echo "== 2/4 灌 resolved (mode, short_uuid, full_uuid) =="
push "$RESOLVED" import_4p_resolved 1,2,3

echo "== 3/4 灌 short (mode, short_uuid, start_time) =="
push "$SHORT" import_4p_short 1,2,3

echo "== 4/4 连接并写入 paipuya_games =="
# start_time 是 unix 秒。走 fromUnixTimestamp64Milli(x*1000) 而不是 toDateTime64(x,3):
# 后者对整数入参的量纲是有歧义的 (秒还是 scale 后的单位), 猜错就是整批数据差 1000 倍。
#
# join_algorithm: 默认的哈希连接会把右表整个装进内存, 两亿行在这里是几个 G, 服务器
# 未必扛得住。grace_hash 会溢写到磁盘, full_sorting_merge 兜底。
ch "INSERT INTO $DB.paipuya_games (uuid, mode_id, started_at, ended_at, players, account_ids, scores)
    SELECT r.uuid,
           r.mode,
           fromUnixTimestamp64Milli(toInt64(s.start_time) * 1000, 'UTC'),
           fromUnixTimestamp64Milli(toInt64(s.start_time) * 1000, 'UTC'),
           [], [], []
    FROM $DB.import_4p_resolved AS r
    INNER JOIN $DB.import_4p_short AS s
      ON r.mode = s.mode AND r.short_uuid = s.short_uuid
    WHERE r.uuid != '' AND s.start_time > 0
    SETTINGS join_algorithm = 'grace_hash,full_sorting_merge'"

echo "== 结果 =="
ch "SELECT count() AS 总数, min(started_at) AS 最早, max(started_at) AS 最新 FROM $DB.paipuya_games"
ch "SELECT mode_id, count() FROM $DB.paipuya_games GROUP BY mode_id ORDER BY mode_id"

echo
echo "临时表还留着, 核对完自己删 (两张加起来约 10GB):"
echo "  DROP TABLE $DB.import_4p_resolved"
echo "  DROP TABLE $DB.import_4p_short"
