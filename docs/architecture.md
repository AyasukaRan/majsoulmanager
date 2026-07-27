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

当前仓库的默认本地模式为了零依赖调试，在 API 进程内完成打包并用内存目录保存索引。`.mjpack` 格式和单条读取路径与生产一致。生产模式应把打包、ClickHouse 批量写入和导出拆成独立 worker。

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
- `played_at` 默认取 payload 首行的 `majsoul.start_time`（unix 秒），`X-Mjai-Played-At` 是显式覆盖。批次内每条记录各有自己的对局时间，不能整批共用一个时间戳。
- 默认单条上限 256KiB，针对解压后的字节数。“单条约 10KB”说的是磁盘上的 gzip 文件；实测 300 条真实 4p 王座记录解压后 min 11,352 / p50 53,668 / p95 80,374 / max 106,157 字节，旧的 16KiB 上限会拒绝其中每一条。
- 生产批量入口使用 tar/tar.zst，每个 member 是一个 `.mjson`；建议单批 10,000–50,000 条或 64–512MB。
- tar member 允许本身是 gzip 的（按内容 magic `1f 8b` 判断，不看文件名）；解压读取以单条上限为界，超过上限的 member 直接拒绝，不会无界分配。采集器磁盘布局因此可以原样打包上传，不必先把 3.2GB 展开成约 34GB。
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

## 容量估算

先用真实样本测量，不用“10KB 上限”直接采购。若平均原文为 5KB：

- 1 亿条约 500GB 原文；10 亿条约 5TB 原文。
- Zstandard 压缩比取决于牌谱字段重复度，必须从至少百万条样本测量。
- 256MB/pack 时，10亿条通常是数万级对象，而不是 10 亿对象。
- ClickHouse 索引容量、分片数和副本数以真实行宽、压缩率、日增量和保留期为输入做压测。

生产建议从 ClickHouse 3 keeper + 2 分片 × 2 副本、RustFS 多节点多盘开始做容量压测，最终拓扑由日写入峰值、查询 SLA 和故障域决定。

## 上线前缺口

- RustFS、Kafka、PostgreSQL、ClickHouse 的生产 adapter。
- 将现有 tar/tar.gz 流式采集 endpoint 接入 Kafka producer，并增加 tar.zst 解码。
- packer/indexer 与 exporter worker 二进制。
- JWT/RBAC、租户隔离、审计日志和限流。
- OpenTelemetry 指标、追踪、DLQ、重放和 orphan GC。
- 真实数据压测、备份恢复演练和 schema 演进策略。
