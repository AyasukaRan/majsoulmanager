# mjai management

[![CI](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/ci.yml)
[![Release](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml/badge.svg)](https://github.com/AyasukaRan/majsoulmanager/actions/workflows/release.yml)

面向数亿级 mjai 对局日志的采集、索引、筛选和批量下载服务。

## 当前能力

- `POST /api/v1/records`：接收单个 mjai/NDJSON 文件，支持幂等键和 SHA-256 校验。
- `POST /api/v1/records/batch`：接收 tar/tar.gz 批次，每个 member 是一份 mjson，member 本身是 gzip 的也直接接收；`played_at` 逐条取自 `majsoul.start_time`。
- `GET /api/v1/records`：按来源、时间、玩家筛选，使用游标分页。
- `GET /api/v1/records/{id}/majsoul-pb`：这条记录转换自的雀魂原始 protobuf，原样返回。内置采集器转换时把 PB 和 mjai 一起存进同一个 pack，索引里 `pb_offset`/`pb_compressed_size`/`pb_size` 三列指向它。转换是有损的——雀魂给的字段远多于 mjai 事件承载的——所以留一份原件，转换器改进之后可以对存档重跑，而不是只对之后收到的记录生效。没有原件的记录返回 `404`：上传进来的本来就是 mjai，转换发生在这个进程看不见的地方；这三列出现之前入库的记录也没有。
- `POST /api/v1/downloads`：创建异步批量导出任务。
- `GET /api/v1/downloads/{job_id}`：查询导出进度和下载地址。
- `GET /api/v1/downloads`：按创建时间倒序列出最近的导出任务。
- `GET /api/v1/stats`：管理台概览的聚合，包含记录总量与近 24 小时增量、按来源的分布、数据包数量与体积、导出任务状态和 Watch 运行状态；计数不使用 FINAL，重放插入后的合并窗口内可能略高。
- `GET /api/v1/stats/series?unit=hour|day&span=N`：管理台趋势页的分桶，缺口补零，永远返回窗口长度那么多个点、最后一个是当前那个桶。`records` 与两个字节数按 `received_at`（进索引的时间）分桶，`games` 按 `played_at`（这局牌开打的时间）分桶——一次历史导入会抬高前者而不动后者。小时桶的 `at` 是 RFC 3339（UTC），天桶是裸日期。窗口有两种写法：`span=N` 表示到当前这个桶为止的 N 个桶；`from`/`to`（RFC 3339，必须成对给）表示一段明确的区间，两端所在的桶都包含在内。上限 `hour` 168、`day` 365，两种写法超出上限都是截断而不是拒绝——区间写法保留 `to` 那一端、砍掉起点。`unit` 认不出来、区间反了、只给了一半，这三种才是 400。同时返回 `rules`：窗口内开打的对局按 `rule` 分组、局数降序，最多 24 项，与 `points[].games` 是同一批记录。计数同样不使用 FINAL。另有 `players`/`room`/`length` 三个逗号分隔的多选筛选（如 `players=4p&room=jade,throne`），分别匹配 `{players}p-{room}-{length}` 的三段：留空表示该段不限、三段全空则不加任何条件，非空的段之间取交集。认不出来的取值和认不出来的 `unit` 一样是 400 而不是被忽略。注意只要筛了任意一段，`rule` 不是这个形状的记录（转换器没写模式、或写了第十三种值）就必然被排除——它们不属于其中任何一段。
- `GET /api/v1/players?q=`：昵称包含该子串的玩家，按对局数降序，最多 50 个。留空表示全部，也就是出场最多的那些。
- `GET /api/v1/players/stats?player=`：一个玩家在某个窗口内的计数。窗口与场次筛选的写法和 `/stats/series` 完全一致（`span=N` 或 `from`/`to`，加 `players`/`room`/`length`）。玩家名是查询参数不是路径段——雀魂昵称里斜杠、问号、百分号都合法。返回的是计数不是比率，分母跟着一起返回：多数指标除以 `hands`，而一发率、里宝率、流听率、役满、最大番数和精算要除以 `detailed_games`——更早转换的记录没有这些字段，那里的 0 表示「不知道」而不是「没发生」。每条记录的每个座位在打包时就被 `src/replay.rs` 计过分写进 `mjai.player_games`，历史记录由启动时的一次性回填补上。
- 内置 majsoul2mjai Watch：在线配置房间、模式、账号密钥引用、代理和轮询频率，展示 UUID 获取与转换状态。
- PB 到 mjai 的转换保留雀魂给出的记分信息：`hora` 带 `fan`/`yakuman`/`riichi`/`yakus`/`yaku_ids`/`dora_markers`/`uradora_markers`，杠翻出的新宝牌指示牌作为 `dora` 事件补上，`ryukyoku` 带 `tenpais` 和每座位的分数变动，`end_game` 带最终点数与精算（`majsoul_result` 里有 `total_point`/`part_point_1`/`grading_score`）。这些字段只出现在此后转换的记录上；更早的记录形状不变，读取方要能同时接受两种。
- 历史牌谱补抓：转换器修好之前入库的记录既没有原始 PB、mjai 也带着已修掉的那些错。它们都带 `majsoul.uuid`，所以可以按 uuid 重新抓一遍、用当前转换器重转、连 PB 一起写进新 pack 并**替换**索引里那一行（`record_id`/`source`/`received_at` 原样保留，靠 ReplacingMergeTree 收敛）。这是管理台上一个独立的服务（「牌谱补抓」页）：自己的账号池、自己的代理、并发会话数和请求间隔都可调，默认不随后端启动。雀魂一个账号只能开一个会话，所以并发上限就是账号池的大小；每次登录前都会重新读一遍采集实例的配置，任何被采集占用的账号都不碰——uuid 只能从 `fetchGameLiveList` 拿到，那个列表只在对局进行中列出它，被踢下线的那段时间里开打又结束的对局，这个部署根本不知道它们存在过；而补抓晚一点跑没有代价。正在跑的采集实例仍然会在自己轮询间隙应答同一个请求队列，属于白拿的产能。只有当重转结果是同一局、能解析、且事件数不少于原记录时才替换。
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

GitHub Actions 分为 `CI` 与 `Release` 两个工作流：

- `CI`：只在 PR 上运行，跑 `Checks`（Rust 与 Web 的格式化、Lint、测试），
  不发布任何产物。这是唯一的门禁。镜像只在 `Dockerfile`、`web/Dockerfile`、
  `Cargo.lock` 或 `web/package-lock.json` 动过时才试构建——`Checks` 已经
  编译过全部 Rust 代码、也跑过 `next build`，镜像构建独有的验证只剩
  Dockerfile 本身。
- `Release`：只在推送 `v*` 标签或手动触发时运行，直接发布镜像到 GHCR。
  它不再重跑 `Checks`——tag 只打在刚合并的 commit 上，那棵树在 PR 上刚被
  同一套检查验证过，重跑一遍是给每次发布白加三分多钟；手动重建旧 tag 时
  更没意义，那是旧代码配今天的工具链。
  - `v*` 标签：同时发布去掉 `v` 前缀的版本标签和 `latest`
  - 手动触发（workflow_dispatch）：只发布版本标签，**不动 `latest`** ——
    重建一个旧版本不应该把 `latest` 拽回去
  - 其他 ref 手动触发时发布 `dev` 标签

  合并到 `main` 本身不再触发构建。每个合并都会跟一个 tag，所以 tag 是
  「这是一次发布」的唯一事件；两个触发都留着的话，同一个 commit 会被完整
  构建两次，产出两套一模一样的镜像。
  - 配置 `DOCKERHUB_USERNAME` 与 `DOCKERHUB_TOKEN` 两个 secrets 后，
    同一批标签会同步推送到 Docker Hub（`docker.io/<用户名>/majsoulmanager-*`）

前端工程说明见 [web/README.md](web/README.md)。

Watch 默认关闭。账号在管理台「账号池」页里填，采集实例的账号引用写
`pool:watch/账号`、补抓池写 `pool:refetch`（补抓的默认值就是它）；密码以 `0600`
存在数据目录里，接口只回 `***`。也可以继续用 `file:...` 或 `env:...` 引用，内容
是一行一个 `username,password`——但 Compose 不挂任何 `/run/secrets` 路径，用
`file:` 得自己加绑定挂载。本地也可以在 `.env` 里设 `MAJSOUL_ACCOUNTS=username,password`
然后在网页选 `env:MAJSOUL_ACCOUNTS`。可以配多条订阅，节点合成一个池子（第二条起的
节点会自动加前缀，免得两家重名）；订阅链接由后端以 `0600` 权限保存在数据目录中，
状态 API 只返回订阅域名和节点数。节点的可用与否探的是雀魂本身而不是能不能上网，
所以「能连上雀魂」这个数才是补抓真正能用的节点数。协议模块的打包和进程接口见
[docs/watch-modules.md](docs/watch-modules.md)。

生产路径不会把每个约 10KB 的 mjson 分别存成 RustFS 对象。打包器把每条记录压成独立 Zstandard frame，再合并为约 256MB 的 `.mjpack`；单条读取根据 ClickHouse 中的 offset/length 发起 Range GET，只下载对应的几 KB。

## 数据目录与迁移

六份持久化数据都是 `MJAI_STORAGE_ROOT`（默认 `./storage`）下的一个目录，不是 Docker
命名卷。它和 `MJAI_DATA_DIR`（默认 `./data`，`make dev` 直接跑在宿主机上时用的那个）
不是一回事，也刻意不共用父目录：这里的子目录是 Docker 守护进程建的，属于 root。

| 子目录 | 内容 | 丢了会怎样 |
| --- | --- | --- |
| `rustfs/` | `.mjpack` 原始语料 | 不可再生 |
| `clickhouse/` | 记录级索引 | 同样不可再生。启动时的补索引只扫 `api/packs`，也就是对象存储出现之前的那批本地语料，它不会枚举 RustFS 重建整个索引 |
| `api/` | 用户、Watch 状态与已发现的 uuid、导出产物、对象存储之前的本地语料 | 账号和 Watch 进度要重来 |
| `postgres/` | 采集幂等状态和下载任务 | 幂等键重置，重传过的批次会被再收一次 |
| `redpanda/` | 已回 `202` 但还没打包的记录 | 丢这一段。停掉采集、等消费追平之后它可以是空的 |
| `mihomo/` | 代理订阅与配置 | 重新填一次订阅；账号绑的节点名会失效，用账号池页的「重新分配节点」重来一次 |

### 语料单独放一块盘

`rustfs/` 是六份里唯一大小跟着语料长的：1.9 亿局按实测每条约两万字节压后是 **3.75
TB**。ClickHouse 和 PostgreSQL 要的是随机 I/O，该留在 SSD 上；rustfs 是大对象顺序
读写，机械盘完全够。所以它有自己的变量：

```bash
# 先搬，再改，最后起 —— 反过来的话新盘是空的，rustfs 会当成一个新的空对象存储
docker compose stop -t 120
docker run --rm -v /data/mjai-storage/rustfs:/from -v /storage:/to alpine:3.21 \
  sh -c 'mkdir -p /to/mjai-rustfs && cp -a /from/. /to/mjai-rustfs/'
# 对一下条数再往下走
echo "MJAI_RUSTFS_ROOT=/storage/mjai-rustfs" >> .env
docker compose up -d
```

不设 `MJAI_RUSTFS_ROOT` 就是 `MJAI_STORAGE_ROOT/rustfs`，也就是这个变量存在之前的
行为，已有部署什么都不用改。

用容器搬而不是 `sudo cp`：语料属于 uid 10001，宿主机上的操作员多半不是它，而 Docker
守护进程本来就是 root。搬完 `storage-perms` 会核对顶层属主。

一个变量，而不是往 `/etc/fstab` 里加一条绑定挂载。后者要 root、写错一行影响开机，而
且**失败是静默的**：挂载没生效 rustfs 就往 `MJAI_STORAGE_ROOT/rustfs` 那个空目录里
写，语料一半在这块盘一半在那块，不报任何错。

绑定挂载不归 Docker 管，所以 `docker compose down -v` 也不会连数据一起删。

### 从命名卷迁过来

这套目录布局之前，六份数据是命名卷。切过来要先把内容搬走，在宿主机上以 root 执行。
**先搬再起**：用新 compose 起过一次的话，新目录里会有空目录甚至新初始化出来的数据，
搬之前要先删掉。

```bash
# 1. 停。给 ClickHouse 和 PostgreSQL 时间刷盘，默认的 10 秒不够
docker compose stop -t 120

# 2. 搬。同一块盘上 mv 是瞬时的，跨盘会退化成拷贝，两种情况都保留属主
export MJAI_STORAGE_ROOT=/srv/mjai
mkdir -p "$MJAI_STORAGE_ROOT"
for name in api postgres clickhouse rustfs redpanda mihomo; do
  mv "$(docker volume inspect -f '{{.Mountpoint}}' "mjai-management_${name}-data")" \
     "$MJAI_STORAGE_ROOT/$name"
done

# 3. 让 .env 指向同一个路径，然后起
echo "MJAI_STORAGE_ROOT=$MJAI_STORAGE_ROOT" >> .env
docker compose pull && docker compose up -d

# 4. 确认记录总量和迁移前一致、趋势页有数据之后，再回收空卷
for name in api postgres clickhouse rustfs redpanda mihomo; do
  docker volume rm "mjai-management_${name}-data"
done
```

### 换机器

```bash
# 语料是不可变追加的，可以在服务还跑着的时候先同步大头，重复几轮到增量足够小
rsync -aHAX --numeric-ids /srv/mjai/rustfs/ 新宿主机:/srv/mjai/rustfs/

# 然后停机，整棵树再同步一次。rustfs 这一轮只剩增量，停机窗口就只有其余五份的大小
docker compose stop -t 120
rsync -aHAX --numeric-ids --delete /srv/mjai/ 新宿主机:/srv/mjai/

# 新宿主机上带着这个仓库和 .env
docker compose up -d
```

`--numeric-ids` 不能省：两台机器上不会有同名的 `postgres`、`clickhouse` 用户，按名字
映射会把属主写成别人，PostgreSQL 直接拒绝启动。迁移期间只用 `stop`，`down -v` 会删掉
还没搬走的命名卷。
