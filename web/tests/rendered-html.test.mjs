import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const projectRoot = new URL("../", import.meta.url);

async function render(path = "/") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  return worker.fetch(
    new Request(`http://mjai.local${path}`, {
      headers: {
        accept: "text/html",
        host: "mjai.local",
        "x-forwarded-proto": "https",
      },
    }),
    {
      ASSETS: {
        fetch: async () => new Response("Not found", { status: 404 }),
      },
    },
    {
      waitUntil() {},
      passThroughOnException() {},
    },
  );
}

test("protects the dashboard and server-renders the login page", async () => {
  const response = await render();
  assert.equal(response.status, 307);
  assert.equal(response.headers.get("location"), "http://mjai.local/login");

  const login = await render("/login");
  assert.equal(login.status, 200);
  assert.match(login.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await login.text();
  assert.match(html, /<title>mjai 管理台<\/title>/i);
  assert.match(html, /登录管理台/);
  assert.match(html, /使用管理员或已验证账号继续/);
  assert.match(html, /创建账号|注册/);
  assert.match(html, /https:\/\/mjai\.local\/og\.png/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape/);
});

test("contains shadcn configuration and no disposable starter", async () => {
  const [components, packageJson] = await Promise.all([
    readFile(new URL("../components.json", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  assert.match(components, /"style": "base-nova"/);
  assert.match(components, /"ui": "@\/components\/ui"/);
  assert.match(packageJson, /"name": "mjai-management-web"/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await access(new URL("../components/ui/button.tsx", import.meta.url));
  await access(new URL("../components/ui/table.tsx", import.meta.url));
  await access(new URL("../public/og.png", import.meta.url));
  await assert.rejects(access(new URL("app/_sites-preview", projectRoot)));
});
