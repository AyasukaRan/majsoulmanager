# curl-chrome 登录模块

用 Chrome 自己的 TLS 栈连雀魂,不起浏览器。

## 为什么

管理台内建的登录走 rustls。同一个端点(`tls.browserleaks.com`)实测,2026-08-17:

| | JA4 | HTTP/2 (akamai) |
|---|---|---|
| 真 Chrome (Playwright) | `t13d1516h2_8daaf6152771_d8a2da3f94cd` | `52d84b11737d980aef856699f885ca86` |
| 内建 (rustls) | `t13d1011h1_61a7ad8aa9b6_3fcd1a44f3e3` | 空 —— 连 h2 都不谈 |
| 本模块 (curl_cffi) | `t13d1516h2_8daaf6152771_d8a2da3f94cd` | `52d84b11737d980aef856699f885ca86` |

三段全不同:10 个密码套件对 15、11 个扩展对 16、ALPN 只报 h1。而登录帧里 `device.f12`
自称是 Chrome —— "自称浏览器但握手证明不是"是判据里最便宜的一条。

帧一级的偏差可以在 Rust 里逐个改(已经改了六类),这一条改不掉:rustls 造不出
Chrome 的 ClientHello。所以把雀魂这段链路挪出进程。

**不用无头浏览器**:那条路每个会话要几分钟、下几十 MB 资源,而在指纹上一分钱都不多买 ——
curl_cffi 和真 Chrome 在 TLS 与 HTTP/2 两层逐字节相同,见上表。

## 部署前提

容器里要有 `python3`(3.10+)和 `curl_cffi>=0.14`:

```dockerfile
RUN pip install --no-cache-dir 'curl_cffi>=0.14'
```

`curl_cffi` 自带 `libcurl-impersonate`,不依赖系统 curl,但**是二进制轮子** —— glibc 的
镜像直接装即可;Alpine/musl 需要确认有对应轮子,否则要换基础镜像。这是采用本模块唯一的
部署代价。

## 安装

控制台的「模块」里上传整个目录,或直接放到数据目录下:

```
<data_dir>/modules/login/curl-chrome/
    manifest.json
    module.py
```

然后在采集设置里把 `login_module` 从 `builtin` 换成 `curl-chrome`。
改完 `manifest.json` 里的 `sha256` 必须跟着更新:

```bash
python3 -c "import hashlib,json,pathlib
sha=hashlib.sha256(pathlib.Path('module.py').read_bytes()).hexdigest()
m=json.loads(pathlib.Path('manifest.json').read_text()); m['sha256']=sha
pathlib.Path('manifest.json').write_text(json.dumps(m,indent=2)+'\n')"
```

## 它和内建实现必须保持一致的地方

两条实现会给同一批账号登录,所以下面这些**必须同值**,不然同一个账号在两条路上
换了硬件,比只用一条还糟:

- `persona()` —— 由账号名派生的 Chrome 版本与视口,和 `src/majsoul/rpc.rs` 的
  `requests::persona` 同表同算法
- `device_id()` —— `login.f5` 与遥测 `device_id`,和 `src/majsoul/mod.rs` 的
  `device_id()` 同算法(SHA-256 前 16 字节摆成 v4 uuid)
- `LOGIN_BEAT_CONTRACT`、`requestConnection` 的 `f6="Web"`、空 body 的 `12 00`

## 自测

```bash
# 协议冒烟(不联网)
echo '{"id":1,"protocol_version":1,"method":"health","params":{}}' | python3 module.py

# 真登录(拿一个账号,被封的号回 503 也算路径通)
python3 - <<'EOF'
import asyncio, json, sys
async def main():
    p = await asyncio.create_subprocess_exec(sys.executable, "module.py",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
    p.stdin.write((json.dumps({"id":1,"protocol_version":1,"method":"open_session",
        "params":{"server":"cn","username":"...","password":"...","proxy_url":None,
                  "client_version":None}})+"\n").encode())
    await p.stdin.drain()
    print(json.loads(await asyncio.wait_for(p.stdout.readline(), 120)))
asyncio.run(main())
EOF
```

`{"ok":false,"error":"...登录失败: 503 account is banned"}` 说明整条链路是通的:
路由拿到了、WS 连上了、握手过了、登录帧被服务端正确解析(否则会是 151 或超时)。
