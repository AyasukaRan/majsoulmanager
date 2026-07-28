# 架构与容量设计

## 关键结论

单个 mjson 约 10KB，但总量达到数亿时，不能在 RustFS 中“一条一个对象”。对象存储用于约 256MB 的不可变 `.mjpack`；每条记录单独压缩成 Zstandard frame，ClickHouse 行索引定位到 frame 的字节范围。

```text
采集器
  │  单条 / 批量 tar
  ▼
Rust API (Axum) ── 幂等状态 ── PostgreSQL
  │
  ▼
Redpanda topic (按 record_id 分区)
  │
  ▼
Pack/Index worker
  ├── 256MB .mjpack ───────────► RustFS
  └── 元数据 + offset/length ──► ClickHouse

查询 API ──► ClickHouse ──► RustFS Range GET ──► 单条 mjson
导出 worker ─► 按 pack_key 合并 Range ─────────► RustFS 导出对象

管理 API ──► Watch supervisor ──► 登录模块 / PB 模块
                            └──► mihomo ──► 雀魂网关
```

采集入口只做校验、幂等占位和写入 Redpanda；打包、上传 RustFS 和批量写 ClickHouse 由 pack/index worker 完成，offset 在写完索引之后才提交。worker 目前是 API 进程内的一组任务（每个 partition 一个），拆成独立二进制是下一步，与之相关的代码已经按“不依赖 AppState 的纯函数 + 一个 worker 结构”组织。

Watch 与查询 API 同镜像部署但属于独立受管任务。配置 revision、运行 phase 和
UUID 队列彼此分离；热重载先校验模块，再替换任务 generation。登录/PB
模块使用独立进程协议，避免 Rust 动态库卸载带来的 ABI 和内存安全风险。
mihomo 控制端口只在容器网络开放，订阅原文不进入普通状态响应。

## `.mjpack` 格式

所有整数采用 big-endian。

| 字段 | 长度 | 说明 |
|---|---:|---|
| 文件 magic `MJPACK01` | 8 bytes | 格式与版本 |
| record UUID | 16 bytes | 可离线重建索引 |
| raw length | 4 bytes | 解压长度及安全上限 |
| compressed length | 4 bytes | frame 长度 |
| Zstandard frame | N bytes | 一条完整 mjson |

ClickHouse 的 `pack_offset` 指向 Zstandard frame 起点，而不是 entry header。读取一条记录只需：

1. 查询 `pack_key, pack_offset, compressed_size, raw_size`。
2. 对 RustFS 请求 `Range: bytes=offset-(offset+compressed_size-1)`。
3. 解压一个独立 frame，并验证 raw size 和 SHA-256。

包内每条独立压缩会比整包压缩略损失压缩率，但换来常数级随机读取。后续可把相邻小记录组成 256KB 左右的 block frame，在索引中增加 `frame_inner_offset`，以压缩率换一次小块读取；第一版先保持一条一 frame，逻辑更可靠。

## 采集

- 单条：`POST /api/v1/records`，body 为原始 JSON array 或 NDJSON。
- 每次必须带 `Idempotency-Key` 和 `X-Mjai-Source`；可带 RFC 3339 格式的 `X-Mjai-Played-At`。
- `played_at` 默认取 payload 里 `start_game` 事件上的 `majsoul.start_time`（unix 秒），`X-Mjai-Played-At` 是显式覆盖。批次内每条记录各有自己的对局时间，不能整批共用一个时间戳。
- 默认单条上限 256KiB，针对解压后的字节数。“单条约 10KB”说的是磁盘上的 gzip 文件；实测 300 条真实 4p 王座记录解压后 min 11,352 / p50 53,668 / p95 80,374 / max 106,157 字节，旧的 16KiB 上限会拒绝其中每一条。老部署的 `.env` 里如果仍写死 16384，升级镜像不会改动它，因此启动时会针对低于 128KiB 的上限打一条警告。
- 生产批量入口使用 tar/tar.zst，每个 member 是一个 `.mjson`；建议单批 10,000–50,000 条或 64–512MB。
- tar member 允许本身是 gzip 的（按内容 magic `1f 8b` 判断，不看文件名）；解压读取以单条上限为界，超过上限的 member 直接拒绝，不会无界分配。采集器磁盘布局因此可以原样打包上传，不必先把 3.2GB 展开成约 34GB。归档本身和 member 都按多流 gzip 解压：拼接出来的 gzip 是合法文件，只读第一段会静默丢掉后面的内容。
- 批次响应区分三种结局：有记录被接收就是 `202`（坏 member 记在 `errors` 里）；一条都没被接收且存在被拒 member 时是 `422`，避免整批格式不对的导入连着几小时都返回成功；记录写不进 Kafka、或积压超过 `MJAI_KAFKA_MAX_LAG` 时是服务端留不住这条记录而不是 member 有问题，整批以 `5xx` 结束。
- API 只在 Kafka 已确认写入后返回 `202`。消费者上传不可变 pack、批量写 ClickHouse，最后提交 Kafka offset。

幂等 ID 应由 `source + Idempotency-Key` 确定，或由服务端生成 UUIDv5。内容 SHA-256 不同却复用同一幂等键时返回 `409`。PostgreSQL 的幂等表只保留业务允许重试的时间窗口（例如 7–30 天），不要永久保存数亿行。

## 索引与筛选

ClickHouse 是记录级索引的事实来源，表定义见 `migrations/clickhouse/001_records.sql`。首版筛选字段：

- `source`
- `received_at` / `played_at`
- `players`
- `rule`
- `sha256`
- `event_count`

查询必须包含时间范围或受服务端最大时间窗限制，并使用 `(received_at, record_id)` keyset cursor；禁止深度 `OFFSET`。玩家数组使用 bloom filter 跳数索引。实际数据上线后通过 `EXPLAIN indexes = 1` 和真实分布决定是否增加 projection 或物化列。

## 单条与批量下载

单条读取只发一次 RustFS Range GET。批量导出是异步任务：

1. 从 ClickHouse 用 keyset 分页流式读取位置索引。
2. 按 `pack_key` 分组并按 offset 排序。
3. 合并相邻 range（例如间隔小于 64KiB），限制每个 RustFS 对象的并发。
4. 解压、校验后流式写 tar.zst。
5. 导出结果写回 RustFS，API 返回短期 presigned URL。

不能在 API 请求生命周期内同步生成亿级归档，也不能把所有命中 ID 放进 PostgreSQL。任务只保存筛选快照、游标、计数和结果对象键。

## 一致性与故障恢复

- pack key 包含日期、Kafka partition 和 UUID，写入后不可覆盖。
- RustFS 上传成功但 ClickHouse 未写入：对象成为 orphan，由基于提交清单和宽限期的 GC 清理。
- ClickHouse 写入成功但 Kafka offset 未提交：消息会重放；稳定 `record_id` 配合 `ReplacingMergeTree` 收敛重复行。
- `.mjpack` header 包含 UUID 和长度，可以离线扫描 RustFS 重建 ClickHouse 索引。
- SHA-256 用于端到端完整性，不把对象 ETag 当内容哈希。

重放能收敛的前提是排序键在重放前后逐列相同。`received_at` 因此取 Kafka 消息自带的时间戳——由生产者在采集入口打上——而不是消费者的 `Utc::now()`：它同时是分区键和 `ORDER BY` 的第一列，重放时若按当时的时钟重新生成，`ReplacingMergeTree` 会认为那是另一行，于是同一条记录在索引里留下两份而不是收敛成一份。`record_id` 同理，由 API 在占位时分配、存进 PostgreSQL、作为 Kafka message key 一路带下来。`pack_key` 和 `pack_offset` 不在排序键里，允许重放前后不同，旧的那个 pack 变成 orphan 由 GC 回收。

## 运行时依赖与参数

| 事项 | 位置 | 说明 |
|---|---|---|
| 消费位点 | PostgreSQL `kafka_offsets` | rskafka 没有 consumer group，位点必须自己存；放在 PostgreSQL 是因为事务性状态本来就在那里。代价是每个 partition 只能有一个消费者，多副本会互相覆盖位点。 |
| topic 保留 | `deploy/redpanda/bootstrap.yml` | `retention_bytes` / `log_retention_ms` 是 cluster property，写在容器命令行的 `--set` 会被 broker 忽略（只在 redpanda.yaml 留一行日志）。bootstrap 文件只在数据卷为空的首次启动读取，之后改用 `rpk cluster config set`。按 641,475 局历史导入、单条 p50 53,668 字节估算约 34.4GB，配 40GiB 上限。 |
| 积压上限 | `MJAI_KAFKA_MAX_LAG`（默认 50000） | 后台每 5 秒采样一次 high watermark 与已提交位点之差；超过上限时采集入口直接拒绝，单条和批量都以 `500` 结束——记录没丢，但采集器必须退避到 worker 把 topic 消费下去为止。这样 topic 不会涨过保留上限、把已经回过 `202` 的记录悄悄丢掉。采样有滞后，因此是软上限；设成 `0` 等于关闭采集而不必停进程。 |
| pack 封包 | `MJAI_PACK_TARGET_BYTES` / `MJAI_PACK_MAX_AGE_SECS` / `MJAI_PACK_IDLE_SECS` | 尺寸或年龄先到者封包。只看尺寸的话，低峰期的记录要等攒满 256MB 才可查，按当前采集速率是数周。第三个是追平 topic 之后的封包年龄（默认 30 秒）：消费本身是毫秒级的，滞后来自攒包，而两局之间已经没有东西再来填这个 pack，等满 `MAX_AGE` 只是拖长记录不可查、以及 broker 那一个卷独自持有已回过 `202` 的字节的时间。它不会在有负载时增加对象数——有积压的 worker 永远追不平，尺寸目标照旧决定封包。 |
| orphan 回收 | `MJAI_GC_GRACE_SECS`（默认 24 小时） | 比宽限期年轻的对象一律不删：上传已完成、索引还在路上的 pack，和写入者中途死掉留下的 pack，从外面看完全一样。清单查询失败或返回空时整轮放弃删除——空清单和“索引正常但没数据”同样无法区分，按空清单处理会删光语料。 |

## 容量估算

先用真实样本测量，不用“10KB 上限”直接采购。若平均原文为 5KB：

- 1 亿条约 500GB 原文；10 亿条约 5TB 原文。
- Zstandard 压缩比取决于牌谱字段重复度，必须从至少百万条样本测量。
- 256MB/pack 时，10亿条通常是数万级对象，而不是 10 亿对象。
- ClickHouse 索引容量、分片数和副本数以真实行宽、压缩率、日增量和保留期为输入做压测。

生产建议从 ClickHouse 3 keeper + 2 分片 × 2 副本、RustFS 多节点多盘开始做容量压测，最终拓扑由日写入峰值、查询 SLA 和故障域决定。

## 上线前缺口

- tar.zst 解码（tar 与 tar.gz 已支持）。
- packer/indexer 与 exporter worker 二进制：目前是 API 进程内的任务（每 partition 一个）。位点在 PostgreSQL 且没有任何 rebalance，多副本会从同一行位点各消费一遍、互相覆盖，因此现阶段 API 只能单副本。
- Redpanda 单节点单 partition、无副本：broker 数据卷损坏等于丢掉尚未打包的那一段记录；上线前至少要给这个卷单独的可靠存储或备份。
- 历史 pack 上传对象存储后本地副本保留，容量按两份算。确认对象存储读取无误之前不删，删除动作留给人工。
- JWT/RBAC、租户隔离、审计日志和限流。
- OpenTelemetry 指标、追踪、DLQ 与重放（orphan GC 已实现）。
- 真实数据压测、备份恢复演练和 schema 演进策略。
