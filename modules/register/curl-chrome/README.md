# curl-chrome 注册模块

在控制台里注册雀魂国服账号,用 Chrome 自己的 TLS 栈,不起浏览器。

## 为什么必须是模块

和 [`../../login/curl-chrome`](../../login/curl-chrome/README.md) 同一个理由,而且更硬:

| | JA4 | HTTP/2 (akamai) |
|---|---|---|
| 真 Chrome (Playwright) | `t13d1516h2_8daaf6152771_d8a2da3f94cd` | `52d84b11737d980aef856699f885ca86` |
| 内建 (rustls) | `t13d1011h1_61a7ad8aa9b6_3fcd1a44f3e3` | 空 —— 连 h2 都不谈 |
| 本模块 (curl_cffi) | `t13d1516h2_8daaf6152771_d8a2da3f94cd` | `52d84b11737d980aef856699f885ca86` |

登录还有历史可看,**一个刚出生的账号除了它的握手之外什么都没有**。所以注册没有内建
实现:模块没装就拒绝注册,而不是用一个一眼假的指纹去建号。

## 部署前提

容器里要有 `python3`(3.10+)和 `curl_cffi>=0.14`:

```dockerfile
RUN pip install --no-cache-dir 'curl_cffi>=0.14'
```

`curl_cffi` 自带 `libcurl-impersonate`,但**是二进制轮子** —— glibc 的镜像直接装即可,
Alpine/musl 要先确认有对应轮子。

## 安装

控制台的「模块」里上传,或直接放到数据目录下:

```
<data_dir>/watch/modules/register/curl-chrome/
    manifest.json
    module.py
```

不需要在任何地方"选中"它:注册没有 builtin 可选,装了的那个就是跑的那个。装了两个
会直接报错让人卸掉一个 —— 到底哪个建的号,事后只能从账号本身去猜。

改完 `module.py` 后 `manifest.json` 的 `sha256` 必须跟着更新:

```bash
python3 -c "import hashlib,json,pathlib
sha=hashlib.sha256(pathlib.Path('module.py').read_bytes()).hexdigest()
m=json.loads(pathlib.Path('manifest.json').read_text()); m['sha256']=sha
pathlib.Path('manifest.json').write_text(json.dumps(m,indent=2)+'\n')"
```

## 邮箱凭据

控制台的输入框里**每行一个凭据串**,行首 `#` 跳过。凭据串里含邮箱地址(模块自己抠出来
用于注册),整串是读那个收件箱的钥匙。

```
abcd1234@outlook.com----密码----clientId----refreshToken
```

凭据串**不会**出现在日志或状态接口里 —— 那两个地方只有邮箱地址。

## 一个账号要多久

拟真开着的时候 **3~5 分钟**,时间花在这些地方,每一处都是照实录抓包来的:

| 阶段 | 耗时 | 为什么不能砍 |
|---|---|---|
| 发码 → 收到码 | 0~2 分钟 | 取决于邮箱服务商 |
| 建连 → signup | 30~50 秒 | 真人在填表单等码;实录 33~56s。建连 0.6s 就 signup 是最扎眼的特征之一 |
| login → 设昵称 | 30~55 秒 | 真人在想昵称;实录相隔 50.5s |
| 设完昵称 → 断开 | 60~120 秒 | 真实会话在 `loginSuccess` 之后还活着 100s+ |

全程还有心跳(首拍 5000,再 5 拍 × 0.5s,之后 5~15s 一次)和登录后的一整套大厅拉取。
关掉拟真每个号快 4 分钟左右,但那样的连接是"全程 0 条心跳、2 秒即断、25 个方法里只用了
5 个" —— 服务端一条「收到 `loginSuccess` 却从没收到 `fetchInfo`」就能认出来。留着这个
开关只是为了做对照组。

## 注册出来的号在哪

**每成功一个就立刻写进账号池,状态是停用。** 不是攒到最后一起写 —— 一个建出来没存下的
账号就废了(它的密码只在那次运行里存在过),所以浏览器关掉、进程重启都不能吃掉已经建好的。

在账号池里确认之后自己启用。

## 需要跟别处保持一致的常量

这些跟 `reg_majsoul/register.py` 和内建登录路径是同一批实录抓出来的,改一处就要对齐:

- `CLIENT_VERSION` / `RES_VERSION` —— 过旧则所有认证请求返回 `151 ERR_CLIENT_VERSION`
- `LOGIN_BEAT` 的合约串 —— 5 份抓包 5 个账号完全一致
- `enc_request_connection` 的 `f6="Web"`(大写 W;`device.f7` 才是小写 `web`)
- `pwd_hash` 的 HMAC key `lailai`

## 自测

```bash
# 协议冒烟(不联网)
echo '{"id":1,"protocol_version":1,"method":"health","params":{}}' | python3 module.py

# 参数校验(不联网,不消耗邮箱)
echo '{"id":1,"protocol_version":1,"method":"register","params":{"mailbox":"nope"}}' \
  | python3 module.py
# -> {"id": 1, "ok": false, "error": "RuntimeError: 凭据串里找不到邮箱地址"}
```

真注册会消耗一个邮箱,没有 dry-run —— 发码这一步在雀魂那边就是真的。
