# mjai management

[![CI](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml)
[![Release](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml/badge.svg)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml)

面向数亿级 mjai 对局日志的采集、索引、筛选和批量下载服务。

## 当前能力

- `POST /api/v1/records`：接收单个 mjai/NDJSON 文件，支持幂等键和 SHA-256 校验。
- `POST /api/v1/records/batch`：接收 tar/tar.gz 批次，每个 member 是一份 mjson，member 本身是 gzip 的也直接接收；`played_at` 逐条取自 `majsoul.start_time`。
- `GET /api/v1/records`：按来源、时间、玩家筛选，使用游标分页。
- `POST /api/v1/downloads`：创建异步批量导出任务。
- `GET /api/v1/downloads/{job_id}`：查询导出进度和下载地址。
- `GET /api/v1/downloads`：按创建时间倒序列出最近的导出任务。
- `GET /api/v1/stats`：管理台概览的聚合，包含记录总量与近 24 小时增量、按来源的分布、数据包数量与体积、导出任务状态和 Watch 运行状态；计数不使用 FINAL，重放插入后的合并窗口内可能略高。
- `GET /api/v1/stats/series?unit=hour|day&span=N`：管理台趋势页的分桶，缺口补零，永远返回窗口长度那么多个点、最后一个是当前那个桶。`records` 与两个字节数按 `received_at`（进索引的时间）分桶，`games` 按 `played_at`（这局牌开打的时间）分桶——一次历史导入会抬高前者而不动后者。小时桶的 `at` 是 RFC 3339（UTC），天桶是裸日期。窗口有两种写法：`span=N` 表示到当前这个桶为止的 N 个桶；`from`/`to`（RFC 3339，必须成对给）表示一段明确的区间，两端所在的桶都包含在内。上限 `hour` 168、`day` 365，两种写法超出上限都是截断而不是拒绝——区间写法保留 `to` 那一端、砍掉起点。`unit` 认不出来、区间反了、只给了一半，这三种才是 400。同时返回 `rules`：窗口内开打的对局按 `rule` 分组、局数降序，最多 24 项，与 `points[].games` 是同一批记录。计数同样不使用 FINAL。另有 `players`/`room`/`length` 三个逗号分隔的多选筛选（如 `players=4p&room=jade,throne`），分别匹配 `{players}p-{room}-{length}` 的三段：留空表示该段不限、三段全空则不加任何条件，非空的段之间取交集。认不出来的取值和认不出来的 `unit` 一样是 400 而不是被忽略。注意只要筛了任意一段，`rule` 不是这个形状的记录（转换器没写模式、或写了第十三种值）就必然被排除——它们不属于其中任何一段。
- 内置 majsoul2mjai Watch：在线配置房间、模式、账号密钥引用、代理和轮询频率，展示 UUID 获取与转换状态。
- 登录与 PB 获取采用版本化进程模块，安装时校验 SHA-256 和协议健康状态，可在线切换、失败回滚。
- 集成 mihomo：网页配置订阅、刷新 provider、测试节点延迟并切换 Watch 专用节点。
- 原始数据与索引分离：RustFS 保存不可变 `.mjpack` 数据包，ClickHouse 保存筛选字段和包内偏移。
- PostgreSQL 保存采集幂等状态和下载任务，Kafka/Redpanda 解耦索引与导出工作负载。
- 管理台提供用户登录、管理员用户管理和公开注册开关；公开注册用户必须在 24 小时内完成邮箱验证。

记录索引持久化在 ClickHouse，幂等与下载任务在 PostgreSQL，两套 schema 由 API 启动时幂等应用；
启动时还会扫描 pack 目录，把索引里缺失的记录补回来。规模设计、单条读取原理和上线清单见
[docs/architecture.md](docs/architecture.md)。

## 快速启动

要求 Rust 1.85+，以及可连通的 PostgreSQL、ClickHouse、Redpanda 和 RustFS：数据库不可用时
API 会拒绝启动，而不是提供一个空索引；broker 或对象存储不可用时同样如此，因为采集入口在
Kafka 确认之前不会回 `202`。

```bash
cp .env.example .env
make test-infra
make install
make dev
```

`cargo test` 直接跑真实 SQL、真实 broker 和真实对象存储，需要上面四个容器在 `127.0.0.1` 上
可达，`make test-infra` 起的就是它们；CI 跑同一个 target，不再单独维护一份 service container
定义。

测试接口：

```bash
curl -X POST http://localhost:8000/api/v1/records \
  -H 'Authorization: Bearer change-me' \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Idempotency-Key: collector-a/game-001' \
  -H 'X-Mjai-Source: collector-a' \
  --data-binary @game.mjai
```

响应 `202 Accepted` 表示记录已取得幂等占用并被 Redpanda 确认落盘，仅此而已：它还没有进索引，
查询要等 pack/index worker 封包、上传 RustFS、批量写 ClickHouse 之后才看得到。worker 崩溃时
offset 没有提交，这批记录会重放并收敛到同一行，不会丢也不会变成两条。

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
