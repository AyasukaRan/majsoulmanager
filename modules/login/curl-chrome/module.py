#!/usr/bin/env python3
"""登录模块: 用 Chrome 自己的 TLS 栈连雀魂。

为什么要有这个模块
------------------
管理台内建的登录走 rustls, 而 rustls 造不出 Chrome 的 ClientHello。同一个端点实测:

    真 Chrome      JA4 t13d1516h2_8daaf6152771_d8a2da3f94cd  akamai 52d84b11737d980aef856699f885ca86
    内建 (rustls)  JA4 t13d1011h1_61a7ad8aa9b6_3fcd1a44f3e3  akamai （空, 连 h2 都不谈）
    本模块         JA4 t13d1516h2_8daaf6152771_d8a2da3f94cd  akamai 52d84b11737d980aef856699f885ca86

三段全不同, 而登录帧里自称 Chrome —— "自称浏览器但握手证明不是"是最廉价的判据。
帧一级的偏差可以逐个改, 这一条在 Rust 里改不掉, 所以把它挪出进程。

不用无头浏览器: 那条路每个会话要几分钟、下几十 MB 资源, 而指纹上一分钱都不多买
(curl_cffi 和真 Chrome 在 TLS 与 HTTP/2 两层逐字节相同, 见上表)。

协议
----
stdin/stdout 上的 JSON 行, 就是 watch_service.rs 里 PluginWorker 说的那套:
    <- {"id":N,"protocol_version":1,"method":"...","params":{...}}
    -> {"id":N,"ok":true,"result":{...}}   或   {"id":N,"ok":false,"error":"..."}
方法: health / open_session / rpc / close_session

依赖: curl_cffi>=0.14 (自带 libcurl-impersonate, 不需要系统 curl)
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import sys
import time
import uuid

from curl_cffi import AsyncSession
from curl_cffi.const import CurlHttpVersion

PROTOCOL_VERSION = 1
IMPERSONATE = "chrome142"          # 133a~142 的 JA4 一致; 更早的第三段不同
ORIGIN = "https://game.maj-soul.com"
MS_HOST = ORIGIN

# 客户端 build 里编译好的路由主机。真客户端不做发现 —— 5 份抓包 1389 条 HTTP 里
# version.json / resversion / config.json 零命中, 它直接打这五台。
ROUTE_HOSTS = [
    "https://route-2.maj-soul.com",
    "https://route-3.maj-soul.com:8443",
    "https://route-4.maj-soul.com",
    "https://route-5.maj-soul.com",
    "https://route-6.maj-soul.com",
]
CN_PACKAGE_VERSION = "4.0.45"
CN_CODE_VERSION = "0.16.257"
LOGIN_BEAT_CONTRACT = "DF2vkXCnfeXp4WoGrBGNcJBufZiMN3uP"   # 实录 15/15 一致

MSG_REQ, MSG_RES = 2, 3

# ---- persona: 一个账号一台机器, 由账号名派生, 和管理台 rpc.rs 的 persona() 同表 ----
CHROME_VERSIONS = [149, 150, 151]
MAC_SCREENS = [(1512, 982), (1440, 900), (1728, 1117), (1280, 800), (1920, 1080), (2560, 1440)]


def persona(account: str) -> dict:
    """必须和 Rust 侧 requests::persona 给出同一台机器 —— 同一个账号在两条实现上
    换了硬件, 比任何一条实现单独用都糟。"""
    h = 0xCBF29CE484222325
    for byte in account.lower().encode():
        h = ((h ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    w, screen_h = MAC_SCREENS[(h >> 8) % len(MAC_SCREENS)]
    return {
        "chrome": CHROME_VERSIONS[h % len(CHROME_VERSIONS)],
        "viewport": (w, screen_h - 220 - ((h >> 16) % 40)),
    }


def user_agent(chrome: int) -> str:
    return (f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{chrome}.0.0.0 Safari/537.36")


def device_id(account: str) -> str:
    """和 login.f5 / 遥测 device_id 同值, 且不随重连改变。与 Rust 的 device_id() 同算法。"""
    raw = bytearray(hashlib.sha256(account.lower().encode()).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def http_headers(chrome: int) -> dict:
    """真客户端调雀魂 HTTP API 时带的那一套 (实录 50/50 条全带)。"""
    return {
        "accept": "*/*",
        "content-type": "text/html;charset=UTF-8",
        "origin": MS_HOST,
        "referer": f"{MS_HOST}/",
        "user-agent": user_agent(chrome),
        "sec-ch-ua": f'"Not=A?Brand";v="99", "Google Chrome";v="{chrome}", "Chromium";v="{chrome}"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }


def ws_headers(chrome: int) -> dict:
    """真 Chrome 的 WebSocket 升级头 (本地裸 socket 实测的那十个)。

    注意浏览器在 WS 握手上【不发】sec-ch-ua / sec-fetch-* / accept —— curl_cffi 的
    impersonate 默认会把那一套页面导航头塞进来, 所以调用处必须配 default_headers=False。
    """
    return {
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": user_agent(chrome),
        "Upgrade": "websocket",
        "Origin": ORIGIN,
        "Sec-WebSocket-Version": "13",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9",
        # 实测雀魂网关不接受 (101 响应里没有 Sec-WebSocket-Extensions), 所以
        # libcurl 不解压这件事不构成风险。
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
    }


# ============================ protobuf ============================
def _uvarint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)


def f_str(fn: int, s: str) -> bytes:
    b = s.encode()
    return _uvarint(fn << 3 | 2) + _uvarint(len(b)) + b


def f_bytes(fn: int, b: bytes) -> bytes:
    return _uvarint(fn << 3 | 2) + _uvarint(len(b)) + b


def f_uint(fn: int, v: int) -> bytes:
    return _uvarint(fn << 3) + _uvarint(v)


def _rv(b: bytes, i: int):
    v = s = 0
    while True:
        x = b[i]
        i += 1
        v |= (x & 0x7F) << s
        if not x & 0x80:
            return v, i
        s += 7


def parse_top(b: bytes) -> dict:
    """{字段号: [(wire_type, 值)]}; 值是 int 或 bytes。"""
    out, i = {}, 0
    while i < len(b):
        tag, i = _rv(b, i)
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _rv(b, i)
        elif wt == 2:
            ln, i = _rv(b, i)
            v, i = b[i:i + ln], i + ln
        elif wt == 5:
            v, i = int.from_bytes(b[i:i + 4], "little"), i + 4
        elif wt == 1:
            v, i = int.from_bytes(b[i:i + 8], "little"), i + 8
        else:
            break
        out.setdefault(fn, []).append((wt, v))
    return out


def pwd_hash(password: str) -> str:
    return hmac.new(b"lailai", password.encode(), hashlib.sha256).hexdigest()


def enc_device(p: dict) -> bytes:
    w, h = p["viewport"]
    return (f_str(1, "pc") + f_str(2, "pc") + f_str(3, "mac") + f_uint(5, 1)
            + f_str(6, "Chrome") + f_str(7, "web") + f_uint(10, w) + f_uint(11, h)
            + f_str(12, user_agent(p["chrome"])) + f_uint(13, 1))


def enc_login(email: str, password_hash: str, device: bytes, did: str,
              code_version: str, package_version: str, tag: str) -> bytes:
    return (f_str(1, email) + f_str(2, password_hash) + f_uint(3, 0)
            + f_bytes(4, device) + f_str(5, did)
            + f_bytes(6, f_str(1, code_version) + f_str(2, package_version))
            + f_uint(7, 1)
            + b"".join(f_uint(8, p) for p in (1, 2, 5, 6, 8, 10, 11))
            + f_uint(9, 0) + f_str(11, f"WebGL_2022-{code_version}") + f_str(12, tag))


def enc_request_connection(route_name: str) -> bytes:
    # f6 是 "Web" 大写 W (实录 3203 576562)。device.f7 才是小写 "web"。
    return f_uint(2, 1) + f_str(3, route_name) + f_uint(4, int(time.time())) + f_str(6, "Web")


def enc_heartbeat(delay: int, quality: int) -> bytes:
    return f_uint(1, delay) + f_uint(2, 0) + f_uint(3, 11) + f_uint(4, quality)


def wrap(method: str, inner: bytes) -> bytes:
    # f2 即使为空也要写 (实录 9 个空 body 的 RPC 全是 1200)
    return f_str(1, method) + f_bytes(2, inner)


def error_of(data: bytes) -> tuple[int, str]:
    """雀魂 ResXxx 的 error 恒在字段 1: {1:错误码, 6:名字}"""
    if not data:
        return 0, ""
    top = parse_top(data)
    if 1 not in top or top[1][0][0] != 2:
        return 0, ""
    err = parse_top(top[1][0][1])
    code = err[1][0][1] if 1 in err else 0
    name = err[6][0][1].decode("utf8", "replace") if 6 in err else ""
    return (code if isinstance(code, int) else 0), name


# ============================ 会话 ============================
class Session:
    """一条活着的雀魂连接。心跳在后台跑, 和真客户端一样一直发到断开。"""

    def __init__(self, sid: str, http: AsyncSession, ws, persona_: dict,
                 code_version: str, package_version: str):
        self.id = sid
        self.http = http
        self.ws = ws
        self.persona = persona_
        self.code_version = code_version
        self.package_version = package_version
        self.mid = 0
        self.pending: dict[int, asyncio.Future] = {}
        self.last_rtt = 5000
        self.reader = asyncio.create_task(self._read())
        self.beater: asyncio.Task | None = None

    async def _read(self) -> None:
        while True:
            raw = await self.ws.recv()
            raw = raw[0] if isinstance(raw, tuple) else raw
            if not isinstance(raw, (bytes, bytearray)) or len(raw) < 3:
                continue
            if raw[0] != MSG_RES:
                continue
            fut = self.pending.pop(int.from_bytes(raw[1:3], "little"), None)
            if fut and not fut.done():
                top = parse_top(raw[3:])
                fut.set_result(top[2][0][1] if 2 in top else b"")

    async def call(self, method: str, inner: bytes, timeout: float = 30.0) -> bytes:
        self.mid = (self.mid + 1) % 60007 or 1
        mid = self.mid
        fut = asyncio.get_running_loop().create_future()
        self.pending[mid] = fut
        await self.ws.send(bytes([MSG_REQ]) + mid.to_bytes(2, "little") + wrap(method, inner))
        await self.ws.flush()          # curl_cffi 的 send 只入队
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.pending.pop(mid, None)

    async def _beat_loop(self) -> None:
        """实录节奏: 建连后 0.5s 连打 5 拍, 之后每 15 秒一直发。
        第一拍 (5000/5000) 由 open_session 在握手后发, 这里从第二拍接上。"""
        try:
            beat = 0
            while True:
                await asyncio.sleep(0.5 if beat < 5 else 15.0)
                beat += 1
                t0 = time.monotonic()
                await self.call(".lq.Route.heartbeat",
                                enc_heartbeat(self.last_rtt, self.last_rtt), timeout=20)
                self.last_rtt = max(1, min(5000, int((time.monotonic() - t0) * 1000)))
        except Exception:
            pass          # 心跳发不出去就是连接没了, 调用方会从自己的请求上看到

    async def close(self) -> None:
        for task in (self.beater, self.reader):
            if task:
                task.cancel()
        try:
            await self.ws.close()
        except Exception:
            pass
        try:
            await self.http.close()
        except Exception:
            pass


SESSIONS: dict[str, Session] = {}


async def fetch_routes(http: AsyncSession, chrome: int, package_version: str) -> list[dict]:
    """按客户端自己的方式问路由 —— 不走 version.json/config.json 那条链, 那三个请求
    实录里一次都没出现过。"""
    last = None
    for host in ROUTE_HOSTS:
        url = (f"{host}/api/clientgate/routes"
               f"?platform=Web&version={package_version}&lang=chs_t")
        try:
            r = await http.get(url, headers=http_headers(chrome), timeout=20)
            routes = (r.json().get("data") or {}).get("routes") or []
            if routes:
                return routes
        except Exception as e:
            last = e
    raise RuntimeError(f"五台 route 主机都没给出路由 (最后一个错误: {last!r})")


async def open_session(params: dict) -> dict:
    username = params["username"]
    password = params["password"]
    proxy = params.get("proxy_url") or None
    tag = params.get("server") or "cn"
    code_version = params.get("client_version") or CN_CODE_VERSION
    package_version = CN_PACKAGE_VERSION
    p = persona(username)
    chrome = p["chrome"]

    http = AsyncSession(impersonate=IMPERSONATE, trust_env=False, timeout=30,
                        **({"proxy": proxy} if proxy else {}))
    try:
        routes = await fetch_routes(http, chrome, package_version)
        domain = routes[0]["domain"]
        route_id = routes[0].get("id") or re.search(r"^([^.:/]+)", domain).group(1)
        gateway = f"wss://{domain}/gateway"

        ws = await asyncio.wait_for(http.ws_connect(
            gateway, headers=ws_headers(chrome), default_headers=False,
            impersonate=IMPERSONATE,
            # WS over h2 (RFC 8441) 网关不支持, 谈成 h2 就再也不回帧。真 Chrome 的
            # WS 握手同样只报 http/1.1, 所以这既是必需也是还原。
            http_version=CurlHttpVersion.V1_1,
            **({"proxy": proxy} if proxy else {})), 25)

        sid = str(uuid.uuid4())
        session = Session(sid, http, ws, p, code_version, package_version)

        code, name = error_of(await session.call(".lq.Route.requestConnection",
                                                 enc_request_connection(route_id)))
        if code:
            await session.close()
            raise RuntimeError(f"requestConnection 被拒: {code} {name}")

        # 每条连接的第一拍恒为 5000/5000 (实录 11/11), 之后交给后台循环
        await session.call(".lq.Route.heartbeat", enc_heartbeat(5000, 5000))
        session.beater = asyncio.create_task(session._beat_loop())

        did = device_id(username)
        started = time.monotonic()
        data = await session.call(".lq.Lobby.login", enc_login(
            username, pwd_hash(password), enc_device(p), did,
            code_version, package_version, tag))
        code, name = error_of(data)
        if code:
            await session.close()
            raise RuntimeError(f"登录失败: {code} {name}".strip())
        account_id = next((v for wt, v in parse_top(data).get(2, []) if wt == 0), 0)

        await settle_into_the_lobby(session)
        SESSIONS[sid] = session

        # 客户端每次登录都上报的四条; 发不出去不影响会话
        if account_id:
            asyncio.create_task(report_login(
                http, p, did, account_id, code_version, package_version,
                domain, time.monotonic() - started))

        return {"session_id": sid, "client_version": f"WebGL_2022-{code_version}"}
    except Exception:
        try:
            await http.close()
        except Exception:
            pass
        raise


async def settle_into_the_lobby(session: Session) -> None:
    """登录后客户端会把大厅画出来。实录的方法序列和间隔, 逐条复刻。
    这些读的结果全部丢弃, 任何一条失败都不影响已经建立的会话。"""
    beat = f_str(1, LOGIN_BEAT_CONTRACT)
    steps = [
        (".lq.Lobby.fetchLastPrivacy", b"", 0.86),
        (".lq.Lobby.loginBeat", beat, 0.34),
        (".lq.Lobby.loginBeat", beat, 1.22),
        (".lq.Lobby.fetchAnnouncement", b"", 5.44),
        (".lq.Lobby.fetchInfo", b"", 0.35),
        (".lq.Lobby.fetchQuestionnaireList", b"", 0.28),
        (".lq.Lobby.fetchChallengeInfo", b"", 0.01),
        (".lq.Lobby.fetchChallengeSeason", b"", 0.01),
        (".lq.Lobby.fetchSeerReportList", b"", 0.01),
        (".lq.Lobby.fetchReviveCoinInfo", b"", 0.04),
        (".lq.Lobby.fetchDailyTask", b"", 0.01),
        (".lq.Lobby.fetchConnectionInfo", b"", 12.28),
        (".lq.Lobby.fetchRollingNotice", b"", 5.00),
        # 最后, 不是最先: 实录里它比 login 晚 51 秒, 和另外三帧同毫秒齐发
        (".lq.Lobby.loginSuccess", b"", 0.89),
    ]
    for method, body, wait in steps:
        await asyncio.sleep(wait)
        try:
            await session.call(method, body)
        except Exception:
            return


TELEMETRY_URL = "https://majsoul-hk-client.cn-hongkong.log.aliyuncs.com/logstores/client/track"
MAC_GPUS = [
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Pro, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M3, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M4, Unspecified Version)",
    "ANGLE (Intel, ANGLE Metal Renderer: Intel(R) UHD Graphics 630, Unspecified Version)",
    "ANGLE (AMD, ANGLE Metal Renderer: AMD Radeon Pro 5500M, Unspecified Version)",
]


async def report_login(http, p, did, account_id, res_version, package_version,
                       gateway_host, seconds) -> None:
    """真客户端每次登录往阿里云 SLS 打的四条。一条都不打 = "登录过但从没上报过的账号",
    那是服务端一次 join 就能拿到的名单。"""
    h = int(hashlib.sha256(did.encode()).hexdigest()[:8], 16)
    base = {
        "APIVersion": "0.6.0", "server": "1", "level": "info",
        "app_runtime_id": str(uuid.uuid4()),
        "res_version": res_version, "client_version": package_version,
        "client_type": "web", "device_type": "pc",
        "device_model": f"Chrome {p['chrome']}.0.0.0",
        "device_os": "MacOS 10.15.7",
        "device_gpu_name": MAC_GPUS[h % len(MAC_GPUS)],
        "device_id": did, "account_id": str(account_id),
    }
    session_id = str(uuid.uuid4())
    lobby = {"session_id": session_id, "connect_lobby": f"{gateway_host}:443"}
    load_ms = int(seconds * 1000)
    lines = [
        (base, "login_stats", json.dumps({"success": True, "use_time": -round(seconds, 3)},
                                         separators=(",", ":"))),
        (base, "game_status", json.dumps({"type": "login_loading_start"}, separators=(",", ":"))),
        ({**base, **lobby}, "certificate_info", '[{"ip":{}}]'),
        ({**base, **lobby}, "game_status",
         json.dumps({"type": "login_loading_end", "load_time": load_ms, "error_code": 0},
                    separators=(",", ":"))),
    ]
    for fields, category, content in lines:
        try:
            await http.get(TELEMETRY_URL,
                           params={**fields, "log_category": category, "content": content},
                           headers=http_headers(p["chrome"]), timeout=10)
        except Exception:
            return


# ============================ 模块协议 ============================
async def handle(method: str, params: dict) -> dict:
    if method == "health":
        return {}
    if method == "open_session":
        return await open_session(params)
    if method == "rpc":
        session = SESSIONS.get(params["session_id"])
        if session is None:
            raise RuntimeError("没有这个 session_id")
        payload = base64.b64decode(params.get("payload_base64") or "")
        answer = await session.call(params["method"], payload)
        return {"payload_base64": base64.b64encode(answer).decode()}
    if method == "close_session":
        session = SESSIONS.pop(params.get("session_id", ""), None)
        if session:
            await session.close()
        return {}
    raise RuntimeError(f"不认识的方法 {method}")


async def main() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    while True:
        line = await reader.readline()
        if not line:
            return
        try:
            request = json.loads(line)
        except Exception:
            continue
        rid = request.get("id")
        try:
            if request.get("protocol_version") != PROTOCOL_VERSION:
                raise RuntimeError(f"protocol_version 必须是 {PROTOCOL_VERSION}")
            result = await handle(request.get("method", ""), request.get("params") or {})
            out = {"id": rid, "ok": True, "result": result}
        except Exception as e:
            out = {"id": rid, "ok": False, "error": f"{type(e).__name__}: {e}"}
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main())
