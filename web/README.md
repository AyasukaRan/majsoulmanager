# mjai management web

基于 React 19、TypeScript、Vinext/Vite、Tailwind CSS 4 和 shadcn/ui 的管理后台。

## 本地开发

```bash
npm install
npm run dev
```

默认访问 `http://localhost:3000`。Rust API 默认位于 `http://localhost:8000`，可通过 `.env` 中的 `MJAI_API_BASE_URL` 调整。

管理台按概览、牌谱索引、Watch 服务、批量导出和用户管理拆分路由。浏览器登录后
仅保存 HttpOnly 会话 Cookie；后端 API key 只由前端 BFF 读取，不会下发到浏览器。
公开注册默认关闭，管理员配置邮件投递服务后可在用户管理页开启；新账号完成邮箱
验证后才能登录。

## 镜像部署

前端使用多阶段 `Dockerfile` 构建 Vinext 产物，并通过 Node 22 运行生产服务器：

```bash
docker build -t mjai-management-web:local .
docker run --rm -p 3000:3000 mjai-management-web:local
```

在仓库根目录执行 `docker compose up -d` 可以同时启动前端、Rust API 和数据基础设施。

## shadcn/ui

配置位于 `components.json`，UI 源码位于 `components/ui/`。新增组件：

```bash
npx shadcn@latest add <component>
```

`lib/mjai-api.ts` 已包含后端记录、存储位置和筛选参数的 TypeScript 类型。正式联调时应通过服务端 BFF 或用户会话访问管理 API，不要把采集器 API key 暴露到浏览器。
