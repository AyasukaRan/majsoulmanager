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
<data_dir>/watch/modules/register/curl-chrome/1.2.0/
    manifest.json
    module.py
```

**版本目录那一层不能少。**少了后端不报错,模块只是**不出现在列表里** —— 看起来像装失败,
其实是装到了没人找的地方。

不需要在任何地方"选中"它:注册没有 builtin 可选,装了的那个就是跑的那个。装了两个
会直接报错让人卸掉一个 —— 到底哪个建的号,事后只能从账号本身去猜。

改完 `module.py` 后 `manifest.json` 的 `sha256` 必须跟着更新:

```bash
python3 -c "import hashlib,json,pathlib
sha=hashlib.sha256(pathlib.Path('module.py').read_bytes()).hexdigest()
m=json.loads(pathlib.Path('manifest.json').read_text()); m['sha256']=sha
pathlib.Path('manifest.json').write_text(json.dumps(m,indent=2)+'\n')"
```

## 邮箱从哪来

三条路,控制台上三选一。给了不止一个时按 `cloud_mail` > `temp_mail` > `mailbox` 取,
只跑一条。分叉只在 `open_mailbox` 里发生一次 —— 三条路的差别就是「地址从哪来」和
「码从哪取」,剩下的完全一样。

### 一、凭据列表(自己备好的邮箱)

控制台的输入框里**每行一个凭据串**,行首 `#` 跳过。凭据串里含邮箱地址(模块自己抠出来
用于注册),整串是读那个收件箱的钥匙。

```
abcd1234@outlook.com----密码----clientId----refreshToken
```

凭据串**不会**出现在日志或状态接口里 —— 那两个地方只有邮箱地址。

邮箱是消耗品,这条路的上限就是买了多少个。

### 二、临时邮箱 API(现开,最省事)

给一个 key 就行:

```
GET /api/generate-email      -> {"success":true,"data":{"email":"hperry371@monity.top"}}
GET /api/emails?email=<地址>  -> {"success":true,"data":{"emails":[...],"count":1}}
```

没有"建邮箱"那一步,也不用问域名 —— **它每次给的地址就换一个域名**,本地名是
「首字母+姓氏+3 位数」的样子。这两件事(域名分散、本地名像真人)自己做都做不好,
而它们正是一批号最容易被捞出来的地方。

实测:开地址 1.2 秒,发码到收到码 **5.3 秒**(paopaodw 那条要 0~2 分钟)。

坑:

- **`X-API-Key` 少了不是 401,是 Cloudflare 一页 403 HTML** —— WAF 挡在应用前面。
  同理这个服务只认像浏览器的客户端:`urllib` 直接打会被 403,我们走 curl_cffi 才过得去。
  所以拿到非 JSON 响应时要报出状态码和内容片段,不然 `json.loads` 抛的错跟真实原因无关。
- 成功判据是 **`success` 字段**,不是 `code`。这一头倒是有真实 HTTP 状态码(401/400)。
- `timestamp` 是 **Unix 秒**,三条路里唯一不用解析时间字符串的 —— 也就没有时区可抄错。
- 正文取 **`content`**:`has_html` 可能是 `true` 而 `html_content` 是空的。
- 额度是**全站共享**的(`usage.remaining_today`),不是你一个人的配额。

**代价是信任**:这些邮箱在服务方眼皮底下,验证码信他们看得见 —— 也就意味着这批号的
找回邮箱不是你独占的。采集号无所谓,别拿它注册你在乎的东西。

### 三、Cloud Mail(现开,自己的实例)

[Cloud Mail](https://github.com/maillab/cloud-mail) 跑在 Cloudflare Workers 上,一个域名
就能开无数个地址。给模块一个实例,它**一个号现开一个邮箱**,再从同一个实例把验证码读回来
—— 于是"注册 20 个号"不再需要先凑 20 个邮箱。

控制台里只要填**实例地址 + 管理员邮箱 + 密码**:

| 填什么 | 说明 |
|---|---|
| 实例地址 | `https://mail.example.com` |
| 管理员邮箱 / 密码 | 换令牌用。已有令牌的话直接填令牌也行(API 里是 `token`) |
| ~~收件域名~~ | **不用填**,开跑前问一次实例,它自己报 |

**这个实例必须是你有管理员账号的。** 开放 API 认令牌、不过人机验证;而"在公共临时邮箱站
上注册个普通用户来收码"那条路走不通 —— 注册要过 Turnstile,而且这类站点多半把「一个账号
多个邮箱」关了(`manyEmail: 1`),一个地址只能注册一个雀魂号。

用到的四个接口([文档](https://doc.skymail.ink/api/api-doc),`websiteConfig` 不在文档里):

- `GET /api/setting/websiteConfig` —— **不需要鉴权**,`domainList` 就是这个实例真的在收信
  的域名(带 `@` 前缀),`minEmailPrefix` 是本地名长度下限。有它才能只填一个地址。
- `POST /api/login` —— 只在域名被藏起来时用,见下。
- `POST /api/public/genToken` —— 拿令牌。**全站只有一个令牌,重新生成会让旧的立刻失效**,
  所以一整批只换一次:每个号各换一次的话,并发起来就是后一个把前一个正在轮询的令牌顶掉。
- `POST /api/public/addUser` —— 建邮箱。**必须在发码之前建好**:收件 worker 对没有对应
  用户的地址默认 `setReject`,邮件连库都进不去,而那个失败长得就像"码一直不来"。
- `POST /api/public/emailList` —— 取码。`toEmail` 走 `LIKE`,不带 `%` 即等值。

`Authorization` 头放**裸令牌**,不是 `Bearer` —— worker 拿这个头的值直接和 KV 里的比对。

四个坑是文档没写的,踩了都不报错:

- 站点可以把域名列表**藏在登录之后**(`loginDomain: 1`),那时 `websiteConfig` 对没有登录态
  的请求返回**空数组**。刚换来的开放 API 令牌顶不上 —— worker 那头是拿 `Authorization`
  当 **JWT** 验的,一个 uuid 令牌验不过,照样是空数组。所以要先 `POST /api/login` 拿 JWT
  再问一次。填了令牌但没填管理员密码的话就登不了,那种情况只能手填域名。
- 业务失败也是 **HTTP 200**,错误在 body 的 `code`/`message` 里。只看状态码会把"令牌不对"
  当成成功。
- `createTime` 是 **UTC**,paopaodw 那边给的是北京时间。抄错时区的表现是每封邮件都被当成
  发码之前的,一路轮询到超时。
- 开关常量是 **0 = OPEN、1 = CLOSE**,反直觉。`"register": 0` 是注册开着。

地址是纯随机小写字母 + 数字尾,长度也随机,多个域名之间随机分 —— 不留批次痕迹。上一批号是
按可辨认的规律取的地址,全灭之后没法排除"就是按名字捞的"这一条。

密码和令牌只在启动那一刻传给模块,不进日志、不进状态接口、浏览器也不存(实例地址和管理员
邮箱会记在本机,密码不会)。

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

echo '{"id":1,"protocol_version":1,"method":"register","params":{}}' | python3 module.py
# -> stage=mailbox "没有任何邮箱来源: mailbox / cloud_mail / temp_mail 给一个"
```

两条现开邮箱的路整条有假服务端的自检(不联网、不碰雀魂):

```bash
python3 reg_majsoul/test_cloud_mail.py \
  mjai_management/modules/register/curl-chrome/module.py
#   Cloud Mail，域名公开: ok
#   Cloud Mail，域名藏在登录之后: ok
#   临时邮箱 API: ok
# 邮箱来源自检通过: module.py
```

同一个脚本也跑 `reg_majsoul/register.py`。两份是同一条协议的两个拷贝,分化了那里会红。

Cloud Mail 那头能不能通,不注册也能验。先看它在收哪些域名(不需要任何凭据):

```bash
curl -s https://mail.example.com/api/setting/websiteConfig | python3 -m json.tool | grep -E 'domainList|minEmailPrefix' -A 5
```

再建一个邮箱试试令牌(重名会报 `emailExistDatabase`,不碰雀魂):

```bash
curl -s https://mail.example.com/api/public/addUser \
  -H 'Authorization: <令牌>' -H 'Content-Type: application/json' \
  -d '{"list":[{"email":"probe123@example.com"}]}'
# -> {"code":200,"message":"success","data":null}
# 令牌不对 -> {"code":401,...}   域名没配 -> {"code":500,"message":"..."}   注意 HTTP 都是 200
```

真注册没有 dry-run —— 发码这一步在雀魂那边就是真的。
