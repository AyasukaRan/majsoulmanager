#!/usr/bin/env python3
"""注册模块: 用 Chrome 自己的 TLS 栈注册雀魂国服邮箱账号。

为什么是模块而不是内建
----------------------
和 `modules/login/curl-chrome` 同一个理由: rustls 造不出 Chrome 的 ClientHello。

    真 Chrome      JA4 t13d1516h2_8daaf6152771_d8a2da3f94cd
    内建 (rustls)  JA4 t13d1011h1_61a7ad8aa9b6_3fcd1a44f3e3   (连 h2 都不谈)
    本模块         JA4 t13d1516h2_8daaf6152771_d8a2da3f94cd

注册比登录更吃这一条: 一个刚出生的账号除了它的握手之外没有任何历史可看。

流程 (还原自真实网页客户端抓包)
------------------------------
  1. POST common-202411.maj-soul.com/api/user/sign_up_code   发验证码
  2. 轮询邮箱取 6 位验证码
  3. GET  route-2.maj-soul.com/api/clientgate/routes         发现 WS 网关
  4. wss://route-*/gateway: .lq.Route.requestConnection -> .lq.Lobby.signup
  5. signup 空响应 = 成功; login 拿 account_id; createNickname 设名

密码算法 HMAC-SHA256(key="lailai", 明文) -> 64 hex。发码时间窗内无图形验证码。

协议
----
stdin/stdout 上的 JSON 行, `watch_service.rs` 里 `PluginWorker` 那套:
    <- {"id":N,"protocol_version":1,"method":"...","params":{...}}
    -> {"id":N,"ok":true,"result":{...}}   或   {"id":N,"ok":false,"error":"..."}
方法: health / register

一次调用注册一个账号, 由管理台循环 —— 进度是逐个报出来的, 而一次注册连取码带
拟真要几分钟, 攒成一批只会让人盯着一个不动的进度条。

依赖: curl_cffi>=0.14 (自带 libcurl-impersonate, 不需要系统 curl)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import secrets
import string
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from curl_cffi import AsyncSession
from curl_cffi.const import CurlHttpVersion

PROTOCOL_VERSION = 1
IMPERSONATE = "chrome142"          # 133a~142 的 JA4 一致; 更早的第三段不同

# ---- 版本/环境常量 (随雀魂更新变化; 失效时对照最新客户端抓包更新) ----
CLIENT_VERSION = "4.0.45"
RES_VERSION = "0.16.257"                    # 不匹配则所有认证请求返回 151
VERSION_STR = f"WebGL_2022-{RES_VERSION}"
REGION = "cn"
ORIGIN = "https://game.maj-soul.com"
SIGN_UP_CODE_URL = "https://common-202411.maj-soul.com/api/user/sign_up_code"
ROUTES_URL = (
    "https://route-2.maj-soul.com/api/clientgate/routes"
    f"?platform=Web&version={CLIENT_VERSION}&lang=chs_t"
)
CODE_API = "https://query.paopaodw.com/api/GetLastEmails"
# 客户端遥测 (阿里云 SLS)。参数里直接带 account_id, 所以服务端不需要任何指纹关联:
# 拿账号表 join 一下日志表就知道这个号从没跑过客户端。
TELEMETRY_URL = "https://majsoul-hk-client.cn-hongkong.log.aliyuncs.com/logstores/client/track"
CN_TZ = timezone(timedelta(hours=8))

MSG_NOTIFY, MSG_REQ, MSG_RES = 1, 2, 3


# ============================ 机器画像 ============================
# 硬件信息在四个地方被上报, 而且【全部是我们自己填的字符串】—— 雀魂不做任何独立采集
# (整个 build 里没有 canvas/WebGL/audio/font hash)。所以可以随机, 但必须整套自洽:
# Windows 的 UA 配 Apple M4 的 GPU 一眼假。四处是 device.f3/f12 · 遥测三项 ·
# HTTP User-Agent · HTTP sec-ch-ua 三件套。
#
# 只做 macOS 内部的多样化, 因为只有 mac 的字段格式有实录抓包为准。加 Windows 画像
# 需要先在对应平台抓一次包确认 device_os 的确切写法 —— 猜格式比不换更容易暴露。
MAC_GPUS = [
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Pro, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M3, Unspecified Version)",
    "ANGLE (Apple, ANGLE Metal Renderer: Apple M4, Unspecified Version)",
    "ANGLE (Intel, ANGLE Metal Renderer: Intel(R) UHD Graphics 630, Unspecified Version)",
    "ANGLE (AMD, ANGLE Metal Renderer: AMD Radeon Pro 5500M, Unspecified Version)",
]
CHROME_VERSIONS = ["149", "150", "151"]
# 常见 mac 屏幕。device 的 f10/f11 是浏览器【视口】而非屏幕分辨率:
# 实录 1512x743 = MacBook Pro 14" (1512x982) 满屏减去浏览器 UI。
MAC_SCREENS = [(1512, 982), (1440, 900), (1728, 1117), (1280, 800), (1920, 1080), (2560, 1440)]


def pick_persona() -> dict:
    """随机一台自洽的 mac。一次注册用一套, 全程不变 —— 同一个号报两台机器比不换更可疑。"""
    ver = secrets.choice(CHROME_VERSIONS)
    w, h = secrets.choice(MAC_SCREENS)
    return {
        "os": "mac",
        "chrome": ver,
        "ua": (
            f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{ver}.0.0.0 Safari/537.36"
        ),
        "platform": '"macOS"',
        "device_os": "MacOS 10.15.7",
        "gpu": secrets.choice(MAC_GPUS),
        # 视口 = 屏幕高减去标签栏/地址栏, 实录 982-743=239
        "viewport": (w, h - 220 - secrets.randbelow(40)),
    }


def headers_for(p: dict) -> dict:
    """Chrome 的完整请求头。抓包里 81/81 条请求都带 sec-ch-ua 三件套; 缺了它们而 UA
    又自称 Chrome 是自相矛盾的。"""
    return {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": ORIGIN,
        "Referer": ORIGIN + "/",
        "User-Agent": p["ua"],
        "sec-ch-ua": (
            f'"Not=A?Brand";v="99", "Google Chrome";v="{p["chrome"]}", '
            f'"Chromium";v="{p["chrome"]}"'
        ),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": p["platform"],
    }


def ws_upgrade_headers(p: dict) -> dict:
    """真 Chrome 的 WebSocket 升级头, 按它自己的顺序。

    本地裸 socket 实测 Chrome 发的就是这一套 —— 它在 WS 握手上【不发】
    sec-ch-ua / Sec-Fetch-* / Accept / Upgrade-Insecure-Requests / Priority。
    curl_cffi 的 impersonate 默认会把这些"页面导航头"全塞进来, 那比裸 python 还扎眼,
    所以必须配 default_headers=False 整套接管。

    唯一对不齐的是 Sec-WebSocket-Key —— libcurl 固定把它放在 Host 之后, 改不了。"""
    return {
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": p["ua"],
        "Upgrade": "websocket",
        "Origin": ORIGIN,
        "Sec-WebSocket-Version": "13",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
    }


# ============================ protobuf 手工编解码 ============================
def _uvarint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(fn: int, wt: int) -> bytes:
    return _uvarint((fn << 3) | wt)


def f_str(fn: int, s: str) -> bytes:
    b = s.encode("utf-8")
    return _tag(fn, 2) + _uvarint(len(b)) + b


def f_bytes(fn: int, b: bytes) -> bytes:
    return _tag(fn, 2) + _uvarint(len(b)) + b


def f_uint(fn: int, v: int) -> bytes:
    return _tag(fn, 0) + _uvarint(v)


def _rv(b: bytes, i: int):
    s = v = 0
    while True:
        x = b[i]
        i += 1
        v |= (x & 0x7F) << s
        if not x & 0x80:
            return v, i
        s += 7


def parse_top(b: bytes) -> dict:
    """只解顶层字段 -> {field_num: [(wire_type, value)]}; length-delimited 保留原始 bytes。"""
    out: dict = {}
    i, n = 0, len(b)
    while i < n:
        try:
            tag, i = _rv(b, i)
        except IndexError:
            break
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _rv(b, i)
        elif wt == 2:
            ln, i = _rv(b, i)
            v, i = b[i:i + ln], i + ln
        elif wt == 5:
            v, i = b[i:i + 4], i + 4
        elif wt == 1:
            v, i = b[i:i + 8], i + 8
        else:
            break
        out.setdefault(fn, []).append((wt, v))
    return out


def error_of(data: bytes) -> tuple[int, str]:
    """雀魂 ResXxx 的 error 恒在字段 1 (成功时缺省): {1:错误码, 6:名字}。返回 (码, 名)。"""
    if not data:
        return 0, ""
    top = parse_top(data)
    if 1 in top and top[1][0][0] == 2:
        err = parse_top(top[1][0][1])
        code = err[1][0][1] if 1 in err and err[1][0][0] == 0 else 0
        name = err[6][0][1].decode("utf-8", "replace") if 6 in err and err[6][0][0] == 2 else ""
        return code, name
    return 0, ""


def check_error(data: bytes) -> int:
    return error_of(data)[0]


def extract_account_id(login_data: bytes) -> int | None:
    """ResLogin.account_id 在字段 2 (varint)。"""
    try:
        wt, v = parse_top(login_data)[2][0]
        return v if wt == 0 else None
    except Exception:
        return None


# ============================ 消息构造 ============================
def pwd_hash(password: str) -> str:
    return hmac.new(b"lailai", password.encode(), hashlib.sha256).hexdigest()


def enc_device(p: dict) -> bytes:
    # 抓包 device: f1=pc f2=pc f3=os f5=is_browser f6=Chrome f7=web f10/f11=视口 f12=UA f13=1
    w, h = p["viewport"]
    return (
        f_str(1, "pc") + f_str(2, "pc") + f_str(3, p["os"]) + f_uint(5, 1)
        + f_str(6, "Chrome") + f_str(7, "web") + f_uint(10, w) + f_uint(11, h)
        + f_str(12, p["ua"]) + f_uint(13, 1)
    )


def enc_signup(email: str, password_hash: str, code: str, device: bytes) -> bytes:
    # 抓包 ReqSignup: f1=email f2=pwd_hash f3=code f4=1 f5=device f6=版本串 f7=cn
    return (
        f_str(1, email) + f_str(2, password_hash) + f_str(3, code) + f_uint(4, 1)
        + f_bytes(5, device) + f_str(6, VERSION_STR) + f_str(7, REGION)
    )


def enc_login(email: str, password_hash: str, device: bytes, device_id: str) -> bytes:
    # 抓包 ReqLogin: f1=email f2=pwd f3=0 f4=device f5=device_id f6=版本msg f7=1
    #               f8=repeated[..] f9=0 f11=版本串 f12=cn
    # f5 不是一次性随机数: 实录里它和遥测上报的 device_id 是同一个值, 两边必须对得上。
    cv = f_str(1, RES_VERSION) + f_str(2, CLIENT_VERSION)
    parts = (
        f_str(1, email) + f_str(2, password_hash) + f_uint(3, 0)
        + f_bytes(4, device) + f_str(5, device_id) + f_bytes(6, cv) + f_uint(7, 1)
    )
    for x in (1, 2, 5, 6, 8, 10, 11):
        parts += f_uint(8, x)
    return parts + f_uint(9, 0) + f_str(11, VERSION_STR) + f_str(12, REGION)


def enc_create_nickname(nick: str) -> bytes:
    return f_str(1, nick) + f_str(3, REGION)


def enc_heartbeat(rtt_ms: int) -> bytes:
    # 实录 0888271000180b208827 = f1/f4=客户端实测 RTT(ms) f2=0 f3=11
    return f_uint(1, rtt_ms) + f_uint(2, 0) + f_uint(3, 11) + f_uint(4, rtt_ms)


def enc_request_connection(route_name: str) -> bytes:
    # 抓包: f2=1 f3=route名 f4=unix时间戳 f6=平台
    # ⚠ f6 是 2026-08 新增的; 缺了它握手仍返回成功, 但之后所有认证请求一律返回 151。
    # f6 是 "Web" 大写 W (实录 3203 576562)。device.f7 才是小写 "web"。
    return f_uint(2, 1) + f_str(3, route_name) + f_uint(4, int(time.time())) + f_str(6, "Web")


def wrap(method: str, inner: bytes) -> bytes:
    return f_str(1, method) + f_bytes(2, inner)


def req_frame(msg_id: int, method: str, inner: bytes) -> bytes:
    return bytes([MSG_REQ]) + msg_id.to_bytes(2, "little") + wrap(method, inner)


# ---- 真实客户端的会话行为 (方法与参数全部来自实录) ----
LOGIN_BEAT = f_str(1, "DF2vkXCnfeXp4WoGrBGNcJBufZiMN3uP")   # 内嵌常量: 5 份抓包完全一致

POST_LOGIN = [
    (".lq.Lobby.fetchLastPrivacy",       "08010802"),
    (".lq.Lobby.fetchAnnouncement",      "0a056368735f741203776562"),
    (".lq.Lobby.fetchInfo",              ""),
    (".lq.Lobby.fetchQuestionnaireList", "0a056368735f741203776562"),
    (".lq.Lobby.fetchChallengeInfo",     ""),
    (".lq.Lobby.fetchChallengeSeason",   ""),
    (".lq.Lobby.fetchSeerReportList",    ""),
    (".lq.Lobby.fetchReviveCoinInfo",    ""),
    (".lq.Lobby.fetchDailyTask",         ""),
    (".lq.Lobby.fetchConnectionInfo",    ""),
    (".lq.Lobby.fetchRollingNotice",     "0a056368735f74"),
]
POST_NICK = [
    (".lq.Lobby.updateAccountSettings", "0a0408011001"),
    (".lq.Lobby.loginSuccess",          ""),
    (".lq.Lobby.fetchAchievementRate",  ""),
    (".lq.Lobby.updateCharacterSort",   "10c19a0c10c29a0c"),   # 默认角色 200001/200002
]
# 没抄 .lq.Lobby.readAnnouncement: 它的参数是公告 id 列表, 得从 fetchAnnouncement 的响应
# 里解出来。照搬实录里的 id = 上报了一批与本账号无关的公告, 比不发更可疑。


# ============================ HTTP ============================
def _no_proxy(kw: dict) -> dict:
    # 显式 proxy=None 在 curl_cffi 里语义和"不传"不同, 所以直接不传。
    if kw.get("proxy") is None:
        kw.pop("proxy", None)
    return kw


async def send_signup_code(session, email: str, p: dict, proxy: str | None):
    # 手动序列化: JS 的 JSON.stringify 无空格, 体积恒差 3 字节 —— 服务端一句
    # len(body) != len(dumps(parsed, separators=(",",":"))) 就能分出来。
    body = json.dumps({"email": email, "type": "email"}, separators=(",", ":"))
    r = await session.post(
        SIGN_UP_CODE_URL,
        data=body,
        **_no_proxy({"proxy": proxy}),
        headers={**headers_for(p), "Content-Type": "application/json"},
    )
    return r.status_code, r.text


async def fetch_gateways(session, p: dict, proxy: str | None) -> list[str]:
    r = await session.get(ROUTES_URL, headers=headers_for(p), **_no_proxy({"proxy": proxy}))
    routes = json.loads(r.text).get("data", {}).get("routes", [])
    return [f"wss://{rt['domain']}/gateway" for rt in routes if rt.get("domain")]


def telemetry_params(p: dict, account_id: int, device_id: str, gateway: str) -> dict:
    """遥测的公共字段。硬件三项全部取自 persona —— 和 WS device、HTTP 头必须是同一台机器。"""
    return {
        "APIVersion": "0.6.0", "server": "1", "level": "info",
        "app_runtime_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
        "res_version": RES_VERSION, "client_version": CLIENT_VERSION, "client_type": "web",
        "device_model": "Chrome " + p["ua"].split("Chrome/")[1].split(" ")[0],
        "device_os": p["device_os"], "device_type": "pc", "device_gpu_name": p["gpu"],
        "device_id": device_id, "account_id": str(account_id),
        "connect_lobby": gateway.split("//")[1].split("/")[0],
    }


TELEMETRY_LOGS = [
    ("login_stats", lambda: {"success": True,
                             "use_time": -round(20 + secrets.randbelow(6000) / 100, 3)}),
    ("game_status", lambda: {"type": "login_loading_end",
                             "load_time": 15000 + secrets.randbelow(15000), "error_code": 0}),
]


async def send_telemetry(session, p: dict, account_id: int, device_id: str,
                         gateway: str, proxy: str | None) -> None:
    """复刻客户端登录后的 SLS 上报。device_id 必须和 login.f5 是同一个值, 否则两边对不上
    比不发更糟。失败静默 —— 遥测挂了不该让注册失败。"""
    base = telemetry_params(p, account_id, device_id, gateway)
    for cat, mk in TELEMETRY_LOGS:
        params = {**base, "log_category": cat,
                  "content": json.dumps(mk(), separators=(",", ":"))}
        try:
            await session.get(TELEMETRY_URL, params=params, headers=headers_for(p),
                              **_no_proxy({"proxy": proxy}))
        except Exception:
            pass
        await asyncio.sleep(0.1)


# ============================ 取验证码 ============================
def _mail_is_new(date_str: str, since_ts: float) -> bool:
    """邮件时间 (北京时间字符串) 是否在发码之后 (容差 180s); 解析失败则不卡。"""
    try:
        dt = datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
        return dt.timestamp() >= since_ts - 180
    except Exception:
        return True


def _extract_code(body: str) -> str | None:
    m = re.search(r"驗證碼\D*?(\d{4,8})", body) or re.search(r"验证码\D*?(\d{4,8})", body)
    if m:
        return m.group(1)
    m = re.search(r"(?<!\d)(\d{6})(?!\d)", body)
    return m.group(1) if m else None


async def fetch_code(session, credential: str, p: dict, since_ts: float,
                     tries: int, interval: float) -> str:
    """轮询 paopaodw 收件箱, 取本次发码之后的雀魂验证码邮件里的 6 位码。"""
    headers = {"Accept": "application/json, text/plain, */*",
               "Referer": "https://query.paopaodw.com/auth.html", "User-Agent": p["ua"]}
    params = {"email": credential, "clientId": "", "refreshToken": "", "num": "3", "boxType": "3"}
    last_msg = ""
    for _ in range(tries):
        try:
            r = await session.get(CODE_API, params=params, headers=headers)
            j = json.loads(r.text)
        except Exception as e:
            last_msg = repr(e)
            await asyncio.sleep(interval)
            continue
        if j.get("code") != 200:
            last_msg = str(j.get("message"))
            await asyncio.sleep(interval)
            continue
        for mail in (j.get("data") or {}).get("inbox", []) or []:
            frm, sub = mail.get("From", ""), mail.get("Subject", "")
            if "majsoul" not in frm.lower() and "雀魂" not in frm:
                continue
            if "驗證碼" not in sub and "验证码" not in sub:
                continue
            if not _mail_is_new(mail.get("Date", ""), since_ts):
                continue
            code = _extract_code(mail.get("Body", ""))
            if code:
                return code
        await asyncio.sleep(interval)
    raise TimeoutError(f"轮询 {tries} 次未取到验证码 (最后: {last_msg})")


# ============================ 生成 ============================
def gen_password(n: int = 12) -> str:
    """纯字母数字 (雀魂登录框前端会拒特殊字符); 保证含大小写+数字。"""
    chars = string.ascii_letters + string.digits
    while True:
        pw = "".join(secrets.choice(chars) for _ in range(n))
        if any(c.isupper() for c in pw) and any(c.islower() for c in pw) and any(c.isdigit() for c in pw):
            return pw


def gen_nickname() -> str:
    """大写字母开头 + 小写字母 + 数字后缀, 组合空间极大, 几乎不会撞名。"""
    head = secrets.choice(string.ascii_uppercase)
    mid = "".join(secrets.choice(string.ascii_lowercase) for _ in range(secrets.choice((3, 4, 5))))
    tail = "".join(secrets.choice(string.digits) for _ in range(secrets.choice((3, 4))))
    return head + mid + tail


def address_of(credential: str) -> str | None:
    """从一行凭据串里抠出邮箱地址; 整串作为取码凭据。"""
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", credential.strip())
    return m.group(0) if m else None


# ============================ 注册流程 ============================
async def signup_flow(call, route_name: str, gw: str, *, email: str, password_hash: str,
                      code: str, device: bytes, device_id: str, nickname: str | None,
                      mimic: bool, telemetry) -> dict:
    """注册流程编排, 与传输层无关 —— 只要给一个 call(method, inner) -> bytes。

    mimic=True 复刻真实客户端会话形态 (心跳/大厅拉取/停顿/遥测), 每号约 2~4 分钟。
    只发 signup+login+createNickname 的连接是全程 0 条心跳、2 秒即断、25 个方法里
    只用了 5 个 —— 服务端一条"收到 loginSuccess 却从没收到 fetchInfo"就能认出来。"""
    async def beat() -> None:
        """全程心跳。实录节奏: 首拍占位 5000, 再 5 拍 × 0.5s, 之后 5~15s 一次。"""
        try:
            await call(".lq.Route.heartbeat", enc_heartbeat(5000))
            for _ in range(5):
                await asyncio.sleep(0.5)
                await call(".lq.Route.heartbeat", enc_heartbeat(30 + secrets.randbelow(90)))
            while True:
                await asyncio.sleep(secrets.choice((5, 13, 15, 15, 15)))
                await call(".lq.Route.heartbeat", enc_heartbeat(30 + secrets.randbelow(90)))
        except Exception:
            pass              # 连接关掉时静默退出

    async def burst(steps) -> None:
        if not mimic:
            return
        for m, hexs in steps:
            await call(m, bytes.fromhex(hexs))
            await asyncio.sleep(secrets.randbelow(300) / 1000)

    async def pause(lo: int, span: int) -> None:
        if mimic:
            await asyncio.sleep(lo + secrets.randbelow(span))

    hb = None
    try:
        await call(".lq.Route.requestConnection", enc_request_connection(route_name))
        if mimic:
            hb = asyncio.create_task(beat())
        # 真人此刻在填注册表单等验证码 (实录 33~56s); 建连后 0.6s 就 signup 最扎眼
        await pause(30, 20)

        err, ename = error_of(
            await call(".lq.Lobby.signup", enc_signup(email, password_hash, code, device))
        )
        if err != 0:
            # 151 = ERR_CLIENT_VERSION: RES_VERSION 过旧, 抓包对照更新
            return {"ok": False, "stage": "signup", "error": f"{err} {ename}".strip()}
        out = {"ok": True, "account_id": None, "nickname": None}

        await asyncio.sleep(1.3)
        ldata = await call(".lq.Lobby.login", enc_login(email, password_hash, device, device_id))
        lerr, lname = error_of(ldata)
        if lerr != 0:
            # 号已经建出来了, 只是没登上 —— 报成功但把登录错误带回去。
            out["login_error"] = f"{lerr} {lname}".strip()
            return out
        out["account_id"] = extract_account_id(ldata)

        # 登录后客户端会拉一整套大厅数据; 少了这些, 服务端一条规则就能认出脚本
        await burst(POST_LOGIN[:1])
        if mimic:
            await call(".lq.Lobby.loginBeat", LOGIN_BEAT)   # 未设昵称时回 1011, 真客户端也一样
            await asyncio.sleep(1.2)
            await call(".lq.Lobby.loginBeat", LOGIN_BEAT)
            if out["account_id"]:
                await telemetry(out["account_id"], device_id, gw)
        await burst(POST_LOGIN[1:])

        # 真人在想昵称 (实录 login -> createNickname 相隔 50.5s)
        await pause(30, 25)

        # 新账号必须设昵称才能进大厅 (否则卡在设名界面); 撞名/非法则换名重试
        last_nerr = 0
        nick = nickname or gen_nickname()
        for _ in range(5):
            last_nerr = check_error(await call(".lq.Lobby.createNickname", enc_create_nickname(nick)))
            if last_nerr == 0:
                out["nickname"] = nick
                break
            if nickname:      # 用户指定的名字撞了就不乱换
                break
            nick = gen_nickname()
        if last_nerr:         # 仅当最终仍失败才记错误码 (清除重试中途的残留)
            out["nickname_error"] = last_nerr

        if mimic:
            await burst(POST_NICK)
            await call(".lq.Lobby.loginBeat", LOGIN_BEAT)
        else:
            await call(".lq.Lobby.loginSuccess", b"")
        # 设完名秒断也是特征: 真实会话在 loginSuccess 之后还活了 100s+
        await pause(60, 60)
        return out
    finally:
        if hb:
            hb.cancel()


async def over_websocket(session, email: str, password_hash: str, code: str, gateways: list[str],
                         p: dict, nickname: str | None, proxy: str | None, mimic: bool) -> dict:
    """逐个网关试, 直到有一个连上。"""
    last_err = None
    for gw in gateways:
        route_name = re.search(r"//([^.:/]+)", gw).group(1)   # route-2
        ws = None
        try:
            ws = await asyncio.wait_for(
                session.ws_connect(
                    gw, headers=ws_upgrade_headers(p), default_headers=False,
                    impersonate=IMPERSONATE,
                    # WS over h2 (RFC 8441) 雀魂网关不支持, 谈成 h2 就再也不回帧了。
                    # 真 Chrome 的 WS 握手同样只报 http/1.1, 所以这既是必需也是还原。
                    http_version=CurlHttpVersion.V1_1,
                    **_no_proxy({"proxy": proxy}),
                ),
                20,
            )
            mid, pend = 0, {}

            async def reader() -> None:
                while True:
                    raw = await ws.recv()
                    raw = raw[0] if isinstance(raw, tuple) else raw   # curl_cffi 回 (data, flags)
                    if isinstance(raw, (bytes, bytearray)) and raw and raw[0] == MSG_RES:
                        fut = pend.pop(int.from_bytes(raw[1:3], "little"), None)
                        if fut and not fut.done():
                            top = parse_top(raw[3:])
                            fut.set_result(top[2][0][1] if 2 in top else b"")

            async def call(method: str, inner: bytes) -> bytes:
                nonlocal mid
                mid += 1
                fut = asyncio.get_running_loop().create_future()
                pend[mid] = fut
                await ws.send(req_frame(mid, method, inner))
                await ws.flush()          # send 只入队, 不 flush 就永远等不到回复
                return await asyncio.wait_for(fut, 20)

            rt = asyncio.create_task(reader())
            try:
                return await signup_flow(
                    call, route_name, gw,
                    email=email, password_hash=password_hash, code=code,
                    device=enc_device(p), device_id=str(uuid.uuid4()),
                    nickname=nickname, mimic=mimic,
                    telemetry=lambda aid, did, g: send_telemetry(session, p, aid, did, g, proxy),
                )
            finally:
                rt.cancel()
        except Exception as e:
            last_err = repr(e)
            continue
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
    return {"ok": False, "stage": "websocket", "error": last_err or "没有可用的网关"}


async def register(params: dict) -> dict:
    """注册一个账号。凭据串既是邮箱地址的来源, 也是取码用的钥匙。"""
    credential = (params.get("mailbox") or "").strip()
    if not credential:
        raise RuntimeError("缺少 mailbox (取码凭据串, 里面含邮箱地址)")
    address = address_of(credential)
    if not address:
        raise RuntimeError("凭据串里找不到邮箱地址")

    password = params.get("password") or gen_password()
    nickname = params.get("nickname") or None
    proxy = params.get("proxy") or None
    tries = int(params.get("poll_tries") or 40)
    interval = float(params.get("poll_interval") or 3.0)
    mimic = params.get("mimic", True)

    # 一个号一台机器: 发码/网关/WS/遥测全程共用这一套硬件信息。
    p = pick_persona()
    since = time.time()
    # trust_env=False: 代理只认显式传入的。继承环境变量会让本该直连的悄悄走上代理。
    async with AsyncSession(impersonate=IMPERSONATE, trust_env=False, timeout=30) as session:
        status, text = await send_signup_code(session, address, p, proxy)
        # 发码失败也是 HTTP 200, 错误在 body 里 (已注册 = ERR_ACC_DUPLICATE_SIGN_UP)。
        # 不看 body 就会白等一整轮取码轮询。
        try:
            api_error = json.loads(text).get("error")
        except Exception:
            api_error = None
        if status != 200 or api_error:
            return {"ok": False, "email": address, "stage": "send_code",
                    "error": f"HTTP {status} {text[:200]}"}

        try:
            code = await fetch_code(session, credential, p, since, tries, interval)
        except Exception as e:
            return {"ok": False, "email": address, "stage": "fetch_code", "error": repr(e)}
        if not re.fullmatch(r"\d{4,8}", code or ""):
            return {"ok": False, "email": address, "stage": "fetch_code",
                    "error": f"验证码异常: {code!r}"}

        gateways = await fetch_gateways(session, p, proxy)
        result = await over_websocket(session, address, pwd_hash(password), code, gateways,
                                      p, nickname, proxy, mimic)
    result["email"] = address
    if result.get("ok"):
        result["password"] = password
    return result


# ============================ 模块协议 ============================
async def handle(method: str, params: dict) -> dict:
    if method == "health":
        return {}
    if method == "register":
        return await register(params)
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
