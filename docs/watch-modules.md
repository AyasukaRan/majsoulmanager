# Mahjong Soul Watch 与热更新模块

Watch 是 `mjai-management-api` 进程内的受管后台任务。API 负责队列、重试、
状态持久化、PB 到 MJAI 转换和 pack 入库；容易随雀魂版本变化的登录与 PB
协议放在独立模块进程中。

内置模块来自 `majsoul2mjai` commit
`da98580990279003f0bf0d636d0d6b8fae19a8cd`，授权与来源见
`NOTICE.majsoul2mjai`。

## 更新流程

1. 管理端上传 manifest 和 base64 编码的单文件可执行程序。
2. 后端校验模块种类、协议版本、名称、版本和 SHA-256。
3. 后端以 `--mjai-module-stdio` 启动新进程，5 秒内完成 `health` 请求。
4. 在线配置选择新版本并热重载 Watch。
5. 新任务使用新模块建立会话；旧任务被取消，其模块进程随之退出。
6. 安装、健康检查或启动失败时保留原配置和原版本。

模块目录按 `watch/modules/{login|pb_fetch}/{name}/{version}` 隔离。不要在
模块 stdout 写日志；stdout 只用于协议，日志写 stderr。模块 stderr 会被逐行
采集进服务日志缓冲并展示在管理台「服务日志」面板（单行截断 8KB），因此
不要在 stderr 回显收到的请求参数（如 `open_session` 的密码、代理凭据）；
后端会对已知机密做替换兜底，但不能覆盖变形输出。

## 通用传输

协议版本为 `1`，使用 stdin/stdout 上逐行 JSON。每个请求和响应必须各占一行；
二进制字段使用标准 base64。

请求：

```json
{"id":1,"protocol_version":1,"method":"health","params":{}}
```

成功响应：

```json
{"id":1,"ok":true,"result":{"version":"2026.07.23"}}
```

失败响应：

```json
{"id":1,"ok":false,"error":"unsupported client version"}
```

模块必须按顺序响应。当前 Watch 对同一个模块实例串行调用。

每个采集实例（`instances[]` 里的一项）各自启动一份模块进程，所以并发采集三麻和四麻时会有多份进程同时存在。模块不需要处理并发请求，但不能假设自己是全局唯一的一份，也不能独占固定端口或固定路径的可写文件。

## Login 模块

Login 模块拥有 WebSocket 会话，支持：

- `open_session`
  - 参数：`server`、`username`、`password`、`proxy_url`、可选
    `client_version`
  - 结果：`session_id`、最终使用的 `client_version`
- `rpc`
  - 参数：`session_id`、`method`、`payload_base64`
  - 结果：`payload_base64`
- `close_session`
  - 参数：`session_id`
  - 结果：空对象

这样网关发现、路由握手、登录字段和心跳在雀魂更新时都能随 Login 模块替换，
而 Watch 状态机不需要改变。

## PB Fetch 模块

PB 模块只负责牌谱相关 protobuf，可与任意 Login 模块组合：

- `build_live_list_request`
  - 参数：`filter_id`
  - 结果：`method`、`payload_base64`
- `parse_live_list_response`
  - 参数：`filter_mode_id`、`payload_base64`
  - 结果：`games` 数组，每项包含 `uuid`、`mode_id`、`start_time`
- `build_record_request`
  - 参数：`uuid`、`client_version`
  - 结果：`method`、`payload_base64`
- `parse_record_response`
  - 参数：`uuid`、`payload_base64`
  - 结果：`pb_base64`

业务错误应返回 `ok:false`，后端会保留 UUID 状态与待处理队列，并按 Watch
重连/重试策略处理。

## manifest

```json
{
  "protocol_version": 1,
  "kind": "login",
  "name": "majsoul-login",
  "version": "2026.07.23",
  "executable": "module",
  "args": [],
  "sha256": "64-character-lowercase-hex"
}
```

模块执行权限由后端设置。在线安装相当于授予该程序后端容器权限，因此安装
接口必须只对受信任管理员开放；生产环境还应在反向代理层启用 RBAC、审计和
模块签名校验。
