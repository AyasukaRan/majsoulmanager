# mjai management

[![CI](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml)
[![Release](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml/badge.svg)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml)

面向数亿级 mjai 对局日志的采集、索引、筛选和批量下载服务。

## 当前能力

- `POST /api/v1/records`：接收单个 mjai/NDJSON 文件，支持幂等键和 SHA-256 校验。
- `POST /api/v1/records/batch`：接收 tar/tar.gz 批次，每个 member 是一份 mjson。
- `GET /api/v1/records`：按来源、时间、玩家筛选，使用游标分页。
- `POST /api/v1/downloads`：创建异步批量导出任务。
- `GET /api/v1/downloads/{job_id}`：查询导出进度和下载地址。
- 内置 majsoul2mjai Watch：在线配置房间、模式、账号密钥引用、代理和轮询频率，展示 UUID 获取与转换状态。
- 登录与 PB 获取采用版本化进程模块，安装时校验 SHA-256 和协议健康状态，可在线切换、失败回滚。
- 集成 mihomo：网页配置订阅、刷新 provider、测试节点延迟并切换 Watch 专用节点。
- 原始数据与索引分离：RustFS 保存不可变 `.mjpack` 数据包，ClickHouse 保存筛选字段和包内偏移。
- PostgreSQL 保存采集幂等状态和下载任务，Kafka/Redpanda 解耦索引与导出工作负载。
- 管理台提供用户登录、管理员用户管理和公开注册开关；公开注册用户必须在 24 小时内完成邮箱验证。

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

Compose 有两套用法：默认直接拉取已发布镜像；本地开发时叠加
`docker-compose.dev.yml` 从当前源码构建。

```bash
# 默认（部署）：拉取 GHCR 已发布镜像（MJAI_IMAGE_TAG 指定版本，默认 latest）
docker compose up -d          # 或 make infra-up；更新镜像用 make infra-pull

# 本地开发：api/web 从当前源码构建（每次 up 自动重建），并把基础设施端口映射到 127.0.0.1
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d   # 或 make infra-up-local
```

两个应用镜像：

- `majsoulmanager-api`：Rust/Axum API，端口 `8000`（本地构建时标签为 `majsoulmanager-api:local`）。
- `majsoulmanager-web`：React + shadcn/ui 管理台，端口 `3000`（本地构建时标签为 `majsoulmanager-web:local`）。
- `metacubex/mihomo:v1.19.27`：Watch 专用代理内核，仅在 Compose 内网暴露控制与代理端口。

宿主机只暴露 `web` 与 `api`，端口可用 `MJAI_WEB_PORT`（默认 `3000`）和
`MJAI_API_PORT`（默认 `8000`）覆盖，与其他服务共用宿主机时在 `.env` 里改掉即可；
PostgreSQL、ClickHouse、RustFS、Redpanda 只在 Compose 内网互通。本地开发叠加 `docker-compose.dev.yml` 时，这些基础设施端口
会额外映射到 `127.0.0.1`（PostgreSQL `5432`、ClickHouse HTTP `8123`/native `9001`、
RustFS S3 `9000`/控制台 `9002`、Redpanda `9092`/Admin `9644`）。

首次启动时会根据 `MJAI_ADMIN_EMAIL` 和 `MJAI_ADMIN_PASSWORD` 创建管理员。
生产部署必须覆盖示例密码。公开注册默认关闭；配置邮件投递 API 后，管理员可在
“用户管理”页面开启注册：

```dotenv
MJAI_PUBLIC_URL=https://mjai.example.com
MJAI_EMAIL_API_URL=https://api.resend.com/emails
MJAI_EMAIL_API_TOKEN=re_xxx
MJAI_EMAIL_FROM=mjai@example.com
```

邮件接口采用 Resend 兼容的 JSON 请求格式。若未配置 `MJAI_EMAIL_API_URL`，
管理台不会允许开启公开注册。

只构建前后端镜像：

```bash
make image-build
```

GitHub Actions 分为 `CI` 与 `Release` 两个工作流，共用 `Checks`（Rust 与 Web
的格式化、Lint、测试）作为门禁：

- `CI`：只在 PR 上运行，执行检查并试构建镜像，不发布任何产物。
- `Release`：只在 push `main`、推送 `v*` 标签或手动触发时运行，检查通过后
  发布镜像到 GHCR：
  - `main`：`ghcr.io/ayasukaran/majsoulmanager-api:latest` 与 `ghcr.io/ayasukaran/majsoulmanager-web:latest`
  - `v*` 标签：发布去掉 `v` 前缀后的版本标签
  - 其他分支仅可手动触发（workflow_dispatch），发布 `dev` 标签
  - 配置 `DOCKERHUB_USERNAME` 与 `DOCKERHUB_TOKEN` 两个 secrets 后，
    同一批标签会同步推送到 Docker Hub（`docker.io/<用户名>/majsoulmanager-*`）

前端工程说明见 [web/README.md](web/README.md)。

Watch 默认关闭。进入管理台填写 `file:/run/secrets/...` 或 `env:...`
形式的账号密钥引用后启用；密钥文件内容为 `username,password`。订阅链接由
后端以 `0600` 权限保存在数据卷中，状态 API 只返回订阅域名。协议模块的打包和
进程接口见 [docs/watch-modules.md](docs/watch-modules.md)。
本地 Compose 可在 `.env` 设置 `MAJSOUL_ACCOUNTS=username,password`，然后
在网页选择 `env:MAJSOUL_ACCOUNTS`；生产环境建议改用只读文件 secret。

生产路径不会把每个约 10KB 的 mjson 分别存成 RustFS 对象。打包器把每条记录压成独立 Zstandard frame，再合并为约 256MB 的 `.mjpack`；单条读取根据 ClickHouse 中的 offset/length 发起 Range GET，只下载对应的几 KB。
