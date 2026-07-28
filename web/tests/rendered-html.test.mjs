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
  assert.match(html, /type="submit"/);
  assert.match(html, /https:\/\/mjai\.local\/og\.png/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape/);
});

test("dashboard pages read from the API instead of literals", async () => {
  const pages = await Promise.all(
    ["page.tsx", "records/page.tsx", "exports/page.tsx"].map((file) =>
      readFile(new URL(`../app/(dashboard)/${file}`, import.meta.url), "utf8"),
    ),
  );
  for (const source of pages) {
    // The placeholder pages were literal arrays. A page that grows one again is
    // indistinguishable from real data on screen, which is the bug being fixed.
    assert.doesNotMatch(source, /428,719,506|1\.84 TB|Asapin|2,841,932/);
  }

  // Every panel has to reach the backend through a proxy route, because the API
  // key those calls need is server-only.
  await Promise.all(
    [
      "app/api/stats/route.ts",
      "app/api/records/[[...path]]/route.ts",
      "app/api/downloads/[[...path]]/route.ts",
    ].map((route) => access(new URL(route, projectRoot))),
  );
});

/**
 * The records table lives behind the session, so this reads the component
 * rather than rendered HTML — the same way the page test above does. What it
 * guards is that the rule filter offers the whole domain the indexer can write
 * and that every option carries Chinese wording, because a dropdown missing a
 * mode silently makes that mode unfilterable.
 */
test("the record index filters on the twelve rules and names them in Chinese", async () => {
  const source = await readFile(
    new URL("../components/record-index.tsx", import.meta.url),
    "utf8",
  );
  const modes = ["gold", "jade", "throne"].flatMap((room) =>
    ["east", "south"].flatMap((length) =>
      [3, 4].map((players) => `${players}p-${room}-${length}`),
    ),
  );
  for (const mode of modes) {
    const labelled = new RegExp(`"${mode}": "[^"]*[\\u4e00-\\u9fff][^"]*"`);
    assert.match(source, labelled, `${mode} has no Chinese label`);
  }
  // The token itself, not another dash: an unrecognised rule has to stay legible.
  assert.match(source, /RULE_LABELS\[record\.rule\] \?\? record\.rule/);
  // The dropdown is fed from that same map, so it cannot drift out of it.
  assert.match(source, /Object\.entries\(RULE_LABELS\)/);
  assert.match(source, /<option value="">全部<\/option>/);
  assert.match(source, /rule: filters\.rule/);
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
