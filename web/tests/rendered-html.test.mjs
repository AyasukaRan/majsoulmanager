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
      "app/api/stats/series/route.ts",
      "app/api/records/[[...path]]/route.ts",
      "app/api/downloads/[[...path]]/route.ts",
    ].map((route) => access(new URL(route, projectRoot))),
  );
});

/**
 * The charts are drawn in SVG by hand. That is a decision worth pinning: the
 * smallest charting library that draws four daily lines is larger than the rest
 * of the console's runtime put together, and it would arrive to replace about a
 * hundred lines of path building. This fails the day one is added anyway.
 */
test("the trend page charts without a charting dependency", async () => {
  const [page, chart, shell, packageJson] = await Promise.all(
    [
      "app/(dashboard)/trends/page.tsx",
      "components/stats-trends.tsx",
      "components/app-shell.tsx",
      "package.json",
    ].map((file) => readFile(new URL(file, projectRoot), "utf8")),
  );

  assert.match(page, /<StatsTrends \/>/);
  assert.match(shell, /href: "\/trends"/);
  // No admin flag: the charts are aggregates of the same index every member can
  // already search, and the endpoint behind them changes nothing.
  assert.doesNotMatch(shell, /href: "\/trends".+admin: true/);
  assert.doesNotMatch(
    packageJson,
    /recharts|chart\.js|apexcharts|echarts|victory|nivo|visx|d3-shape/,
  );
  assert.match(chart, /<svg/);
  assert.match(chart, /viewBox=/);
  // Drawn at device pixels. The first version put a 760-unit viewBox under
  // `w-full`, so the card's width scaled the labels to 17px and the strokes to
  // 3px — measuring the container is what keeps a 10px label 10px.
  assert.match(chart, /new ResizeObserver/);
  // The byte axis rounds its step inside the unit the labels use. Without it a
  // storage axis reads 7.45 GiB / 14.90 GiB / 22.35 GiB.
  assert.match(chart, /1024 \*\*/);

  // Both series, both columns. `games` reading `received_at` is the one bug
  // that would leave the chart looking entirely plausible, so the wording that
  // tells the two apart is part of the page rather than a comment.
  assert.match(chart, /key: "records"/);
  assert.match(chart, /key: "games"/);
  assert.match(chart, /导入/);

  // The shortest window is hours, not days: "did the collector stop this
  // afternoon" is a question a day of daily buckets answers with one bar.
  assert.match(chart, /unit: "hour", span: 24/);
  // Hourly buckets arrive as UTC instants and are rendered in the reader's own
  // timezone; daily ones are bare dates and must never be parsed as instants,
  // which would shift them across midnight west of Greenwich.
  assert.match(chart, /if \(unit === "day"\) \{\n    return point\.at;/);
  // The mode breakdown reads the shared label map rather than the raw token.
  assert.match(chart, /ruleLabel/);
  assert.match(chart, /from "@\/lib\/rules"/);
  // The three facets are multi-select and each is sent only when constrained;
  // an omitted facet is what tells the API to add no predicate at all.
  assert.match(chart, /RULE_FACETS/);
  assert.match(chart, /params\.set\(facet\.key/);
  assert.match(chart, /aria-pressed=\{on\}/);

  // The window is not only the presets: a custom range is two native date
  // inputs, and both bounds go out together — half a range would otherwise be
  // silently reinterpreted as a window ending now.
  assert.match(chart, /自定义/);
  assert.match(chart, /type="date"/);
  assert.match(chart, /params\.set\("from", dayStart/);
  assert.match(chart, /params\.set\("to", dayEnd/);
  // A window switch swaps 365 points for 7 under a chart that keeps its hover
  // index, and an unclamped index reads past the end of the array.
  assert.match(chart, /Math\.min\(hovered, points\.length - 1\)/);

  // The readout has to be reachable without a pointer, and the chart has to let
  // the page scroll: `touch-action: none` on something this tall means a finger
  // landing on it scrolls nothing at all.
  assert.match(chart, /touch-pan-y/);
  assert.doesNotMatch(chart, /touch-none/);
  assert.match(chart, /tabIndex=\{0\}/);
  assert.match(chart, /ArrowLeft/);
  assert.match(chart, /aria-live="polite"/);
});


/**
 * The facet values are substrings of the rule token that the API parses and
 * rejects by name, so this file and `RulePlayers`/`RuleRoom`/`RuleLength` in
 * `src/catalog.rs` have to agree exactly. A value only the console knows about
 * is a filter that 400s on click.
 */
test("the mode facets match the tokens the API will accept", async () => {
  const [rules, catalog] = await Promise.all([
    readFile(new URL("lib/rules.ts", projectRoot), "utf8"),
    readFile(new URL("../src/catalog.rs", projectRoot), "utf8"),
  ]);
  const facets = {
    RulePlayers: ["3p", "4p"],
    RuleRoom: ["gold", "jade", "throne"],
    RuleLength: ["east", "south"],
  };
  for (const [name, values] of Object.entries(facets)) {
    const declared = new RegExp(`rule_facet!\\(${name} \\{([^}]*)\\}`).exec(catalog);
    assert.ok(declared, `${name} is not declared with rule_facet!`);
    const tokens = [...declared[1].matchAll(/"([^"]+)"/g)].map((hit) => hit[1]);
    assert.deepEqual(
      tokens.sort(),
      [...values].sort(),
      `${name} in catalog.rs is not the set the console offers`,
    );
  }

  // Both directions. Containment alone would let the console offer a value the
  // API has never heard of, which is a filter that 400s the moment it is
  // clicked — exactly the failure this test says it prevents. `value: "` occurs
  // only inside RULE_FACETS, so the extraction is exact.
  assert.deepEqual(
    [...rules.matchAll(/value: "([^"]+)"/g)].map((hit) => hit[1]).sort(),
    Object.values(facets).flat().sort(),
    "lib/rules.ts offers a facet value the API will reject",
  );
});

/**
 * The theme shipped `--chart-1..5` as a greyscale ramp in dark mode, which is
 * invisible as a design decision right up until something plots two series with
 * it: both were the same grey, and the swatch in the legend named a shade
 * rather than a line. Nothing but the charts reads these tokens, so this pins
 * that the dark ones carry colour.
 */
test("the dark theme gives the chart palette actual colours", async () => {
  const css = await readFile(new URL("app/globals.css", projectRoot), "utf8");
  const dark = css.slice(css.indexOf(".dark {"));
  assert.notEqual(dark.indexOf(".dark {"), -1);
  for (let token = 1; token <= 5; token += 1) {
    const declared = new RegExp(
      `--chart-${token}:\\s*oklch\\(([\\d.]+)\\s+([\\d.]+)`,
    ).exec(dark);
    assert.ok(declared, `--chart-${token} is not an oklch() in the dark theme`);
    const [, lightness, chroma] = declared;
    assert.ok(
      Number(chroma) > 0.05,
      `--chart-${token} has chroma ${chroma} in the dark theme, i.e. it is grey`,
    );
    // The dark card sits at 0.205; a series drawn below that disappears into it.
    assert.ok(
      Number(lightness) > 0.5,
      `--chart-${token} at lightness ${lightness} is too dark for the card`,
    );
  }
});

/**
 * The records table lives behind the session, so this reads the component
 * rather than rendered HTML — the same way the page test above does. What it
 * guards is that the rule filter offers the whole domain the indexer can write
 * and that every option carries Chinese wording, because a dropdown missing a
 * mode silently makes that mode unfilterable.
 */
test("the record index filters on the twelve rules and names them in Chinese", async () => {
  const [source, rules] = await Promise.all(
    ["components/record-index.tsx", "lib/rules.ts"].map((file) =>
      readFile(new URL(file, projectRoot), "utf8"),
    ),
  );
  const modes = ["gold", "jade", "throne"].flatMap((room) =>
    ["east", "south"].flatMap((length) =>
      [3, 4].map((players) => `${players}p-${room}-${length}`),
    ),
  );
  for (const mode of modes) {
    const labelled = new RegExp(`"${mode}": "[^"]*[\\u4e00-\\u9fff][^"]*"`);
    assert.match(rules, labelled, `${mode} has no Chinese label`);
  }
  // One map, two readers. The trends breakdown names the same tokens, and a
  // second copy would drift the day Majsoul adds a room.
  assert.match(source, /from "@\/lib\/rules"/);
  // Every label is the three facet labels joined, in token order. The first
  // version named the player count only for 三麻 and dropped 之间 only there,
  // so half the list read as a different naming scheme from the other half.
  for (const mode of modes) {
    const labelled = new RegExp(`"${mode}": "([^"]+)"`).exec(rules);
    assert.ok(labelled, `${mode} has no label`);
    assert.equal(
      labelled[1],
      [
        mode.startsWith("4p") ? "四麻" : "三麻",
        { gold: "金之间", jade: "玉之间", throne: "王座之间" }[
          mode.split("-")[1]
        ],
        mode.endsWith("east") ? "东风" : "东南",
      ].join("·"),
      `${mode} is not named as its three facets`,
    );
  }
  // The token itself, not another dash: an unrecognised rule has to stay legible.
  assert.match(source, /RULE_LABELS\[record\.rule\] \?\? record\.rule/);
  // The dropdown is fed from that same map, so it cannot drift out of it.
  assert.match(source, /Object\.entries\(RULE_LABELS\)/);
  assert.match(source, /<option value="">全部<\/option>/);
  assert.match(source, /rule: filters\.rule/);
});

test("a new collector is created watching every ranked room", async () => {
  // Two instances ran for days on the old single-room default and collected
  // 61,934 games from one third of the ladder. A room nobody watched is games
  // nobody can fetch afterwards, so the default is pinned here and in
  // WatchInstance::default, and the two have to say the same thing.
  const source = await readFile(
    new URL("../components/watch/instance-list.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /room: "all"/);
  assert.match(source, /<option value="all">全部<\/option>/);
});

/**
 * The room field sat at its default for days because it did not look like a
 * setting among a page of readings, and an operator following a live collection
 * was one stray click from editing what drove it. Configuration reappearing on
 * the monitoring page is exactly the regression this guards.
 */
test("the monitoring page holds no configuration and points at the settings page", async () => {
  const [watch, settings, shell] = await Promise.all(
    [
      "app/(dashboard)/watch/page.tsx",
      "app/(dashboard)/settings/page.tsx",
      "components/app-shell.tsx",
    ].map((file) => readFile(new URL(file, projectRoot), "utf8")),
  );

  assert.doesNotMatch(watch, /components\/watch\//);
  assert.doesNotMatch(watch, /WatchSettings|WatchServiceCard|WatchProxyCard/);
  assert.match(watch, /href="\/settings"/);

  assert.match(settings, /<WatchSettings \/>/);
  // A member reaching the settings route would be handed the proxy subscription
  // field and the stop button, so the page turns them away rather than only
  // hiding the nav entry.
  assert.match(settings, /user\.role !== "admin"/);
  assert.match(shell, /href: "\/settings".+admin: true/);
});

/**
 * The account pool is filled from the registrar's `accounts.jsonl`, fifty rows
 * at a time, so the reading of that file is the one piece of the console worth
 * asserting on behaviour rather than on source text. What it has to get right
 * is the duplicate rule: the backend rejects the whole document when one
 * account appears twice, case-insensitively, so an import that lets a
 * re-registered address through loses the other forty-nine rows with it.
 */
test("reads the registrar's accounts.jsonl and drops what the pool already has", async () => {
  const { parseAccountsJsonl } = await import("../lib/accounts-jsonl.mjs");
  const report = parseAccountsJsonl(
    [
      String.raw`{"email":"a@example.com","password":"pw-a","nickname":"甲","account_id":null,"ts":1}`,
      "",
      String.raw`{"email":"A@Example.COM","password":"pw-again","nickname":"甲二"}`,
      String.raw`{"email":"b@example.com","password":"pw-b","nickname":null}`,
      String.raw`{"email":"c@example.com","password":"pw-c"}`,
      String.raw`{"email":"d@example.com"}`,
      String.raw`{"email":"e f@example.com","password":"pw-e"}`,
      "这一行不是 JSON",
    ].join("\n"),
    // As the pool holds it — a different spelling of the same login.
    ["C@example.com"],
  );

  assert.deepEqual(report.accounts, [
    { username: "a@example.com", password: "pw-a", note: "甲" },
    // A null nickname is a blank note, not the string "null".
    { username: "b@example.com", password: "pw-b", note: "" },
  ]);
  // The repeat of a@ within the file, and c@ against the pool.
  assert.equal(report.duplicates, 2);
  // No password; a space in the login, which validate() refuses by name; and no
  // JSON at all. The middle one is the point of checking it here: staged, it
  // would take the other forty-nine rows down with it on save.
  assert.equal(report.unusable, 3);

  // Wired into the card, and onto the pool the re-fetch half draws from: an
  // import filed as 实时采集 is an account the collector would fight over.
  const source = await readFile(
    new URL("../components/account-pool.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /parseAccountsJsonl/);
  assert.match(source, /type="file"/);
  assert.match(source, /purpose: "refetch" as const/);
});

/**
 * The concurrency box and the API's own rule have to agree. A console that
 * offers 64 against a backend that refuses anything over 16 is a 400 on save,
 * and the operator's only clue is a number they typed being called invalid.
 */
test("nothing caps the re-fetch concurrency, on either side", async () => {
  const [panel, service] = await Promise.all([
    readFile(new URL("../components/refetch-panel.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../src/refetch_service.rs", import.meta.url), "utf8"),
  ]);
  // The box still refuses zero — a pool of no sessions is a pool that does
  // nothing while reporting that it is running.
  assert.match(panel, /type="number"\n\s+min=\{1\}\n\s+value=\{config\.concurrency\}/);
  assert.doesNotMatch(panel, /max=\{\d+\}\n\s+value=\{config\.concurrency\}/);
  assert.doesNotMatch(service, /MAX_CONCURRENCY/);
  assert.match(service, /self\.concurrency < 1/);
});

/**
 * A batch of fifty imported accounts is a batch of fifty rows to re-file, so
 * the pool edits by selection. What this pins is the one thing that can go
 * quietly wrong: the selection is by position, and a delete shifts every later
 * row up — so a delete that does not clear it re-points every tick at the row
 * that moved into the slot, and the next batch edit lands on the wrong accounts.
 */
test("the account pool edits a selection, and a delete drops it", async () => {
  const source = await readFile(
    new URL("../components/account-pool.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /const editSelected =/);
  assert.match(source, /aria-label="全选"/);
  assert.match(source, /已选 \{selected\.size\} 个/);
  // Every delete goes through the one helper that clears the selection, so
  // neither the row button nor the batch button can forget to.
  assert.match(source, /const removeAt = [\s\S]*?setSelected\(new Set\(\)\);/);
  assert.match(source, /onClick=\{\(\) => removeAt\(\(at\) => at === index\)\}/);
  assert.match(source, /removeAt\(\(at\) => selected\.has\(at\)\)/);
  assert.doesNotMatch(source, /accounts\.filter\(\(_, at\) => at !== index\)/);
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
