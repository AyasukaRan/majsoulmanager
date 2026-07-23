# mjai management

面向数亿级 mjai 对局日志的采集、索引、筛选和批量下载服务。

## 当前能力

- `POST /api/v1/records`：接收单个 mjai/NDJSON 文件，支持幂等键和 SHA-256 校验。
- `POST /api/v1/records/batch`：接收 tar/tar.gz 批次，每个 member 是一份 mjson。
- `GET /api/v1/records`：按来源、时间、玩家筛选，使用游标分页。
- `POST /api/v1/downloads`：创建异步批量导出任务。
- `GET /api/v1/downloads/{job_id}`：查询导出进度和下载地址。
- 原始数据与索引分离：RustFS 保存不可变 `.mjpack` 数据包，ClickHouse 保存筛选字段和包内偏移。
- PostgreSQL 保存采集幂等状态和下载任务，Kafka/Redpanda 解耦索引与导出工作负载。

默认本地后端用于零依赖开发和接口测试；生产环境使用 `.env.example` 中的基础设施配置。规模设计、单条读取原理和上线清单见 [docs/architecture.md](docs/architecture.md)。

## 快速启动

要求 Rust 1.85+。

```bash
cp .env.example .env
make install
make dev
```

测试接口：

```bash
curl -X POST http://localhost:8000/api/v1/records \
  -H 'Authorization: Bearer change-me' \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Idempotency-Key: collector-a/game-001' \
  -H 'X-Mjai-Source: collector-a' \
  --data-binary @game.mjai
```

本地模式响应 `202 Accepted` 时已经写入 pack 并建立内存索引；生产模式将在 Kafka 确认持久化后返回，并由 worker 异步建索引。

批量上传（归档内每个文件独立校验和去重）：

```bash
tar -czf games.tar.gz *.mjson
curl -X POST http://localhost:8000/api/v1/records/batch \
  -H 'Authorization: Bearer change-me' \
  -H 'Content-Type: application/gzip' \
  -H 'Idempotency-Key: collector-a/batch-001' \
  -H 'X-Mjai-Source: collector-a' \
  --data-binary @games.tar.gz
```

## 本地基础设施

```bash
docker compose up -d
```

Compose 会构建并启动两个应用镜像：

- `mjai-management-api:local`：Rust/Axum API，端口 `8000`。
- `mjai-management-web:local`：React + shadcn/ui 管理台，端口 `3000`。

其余本地端口：PostgreSQL `5432`、ClickHouse HTTP `8123`、RustFS S3 `9000`、RustFS 控制台 `9002`、Redpanda `9092`。

只构建前后端镜像：

```bash
make image-build
```

前端工程说明见 [web/README.md](web/README.md)。

生产路径不会把每个约 10KB 的 mjson 分别存成 RustFS 对象。打包器把每条记录压成独立 Zstandard frame，再合并为约 256MB 的 `.mjpack`；单条读取根据 ClickHouse 中的 offset/length 发起 Range GET，只下载对应的几 KB。
