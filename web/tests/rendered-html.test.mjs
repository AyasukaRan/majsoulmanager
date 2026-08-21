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

/**
 * A pool of a thousand accounts is six thousand form controls if the table
 * renders whole, so it renders a page at a time. Two things that costs, both of
 * them silent:
 *
 * every callback in a row addresses that row by its position in the whole pool,
 * so a row carrying its per-page index would have page 2 editing and deleting
 * page 1's accounts; and 全选 is the reason the batch controls are usable at
 * this size at all, so it stays the whole pool rather than quietly becoming
 * "this page" — twenty rounds of a batch edit is not a batch edit.
 */
test("the account pool pages its rows and still selects the whole pool", async () => {
  const source = await readFile(
    new URL("../components/account-pool.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /const PAGE_SIZE = \d+/);
  assert.match(source, /accounts\s*\.slice\(start, start \+ PAGE_SIZE\)/);
  // The row's index is the pool's, not the page's.
  assert.match(source, /index: start \+ offset/);
  assert.match(source, /visible\.map\(\(\{ account, index \}\)/);
  // Clamped where it is read: a delete can drop the page count under the page
  // that is open, and a table that renders blank is a pool that looks lost.
  assert.match(source, /Math\.min\(page, pages - 1\)/);
  // 全选 covers `accounts`, never `visible`.
  assert.match(source, /checked \? new Set\(accounts\.map\(\(_, at\) => at\)\)/);
  assert.doesNotMatch(source, /new Set\(visible\.map/);
});

/**
 * An account can be sent out of its own mihomo node, which is what spreads a
 * pool of eighty sessions over several exits. Two things about the picker are
 * worth pinning, both of them ways to silently re-file somebody's accounts:
 * a node an account already names has to stay in the list even when the
 * subscription no longer offers it — a `<select>` whose value is not among its
 * options renders blank, and the next save would write that blank back — and
 * the batch picker's "follow" option cannot carry the empty string, because
 * that is the placeholder's value and picking it would fire no event at all.
 */
test("the account pool binds accounts to nodes without dropping unknown ones", async () => {
  const [source, api, service] = await Promise.all([
    readFile(new URL("../components/account-pool.tsx", import.meta.url), "utf8"),
    readFile(new URL("../lib/mjai-api.ts", import.meta.url), "utf8"),
    readFile(new URL("../../src/mihomo.rs", import.meta.url), "utf8"),
  ]);
  assert.match(api, /node: string;/);
  assert.match(source, /nodeOptions/);
  // Both halves of the option list: what mihomo offers, and what the accounts
  // already name.
  assert.match(source, /proxy\?\.nodes \?\? \[\]\)\.map\(\(node\) => node\.name\)/);
  assert.match(source, /accounts\.map\(\(account\) => account\.node\)\.filter\(Boolean\)/);
  assert.match(source, /const FOLLOW = "__follow__"/);
  assert.match(source, /picked === FOLLOW \? "" : picked/);
  // The console's ports have to be the ports the backend generates listeners
  // for, so neither side may drift: the group name is what the status keys on.
  assert.match(service, /MAJSOUL-OUT-\{slot\}/);
  assert.match(service, /OUTBOUND_PORT_BASE: u16 = 7900/);
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

test("a sync cursor is placed in the window by time, and clamped to it", async () => {
  const { day, windowPercent } = await import("../lib/paipuya-progress.mjs");
  const start = "2026-01-01T00:00:00Z";
  const end = "2026-01-11T00:00:00Z";

  assert.equal(day("2026-03-12T07:41:09Z"), "2026-03-12");
  assert.equal(windowPercent(start, start, end), 0);
  assert.equal(windowPercent("2026-01-06T00:00:00Z", start, end), 50);
  assert.equal(windowPercent(end, start, end), 100);

  // A cursor outside the window is not an error — 结束时间 moved back after the
  // sweep passed it, or 开始时间 moved forward before it started — and neither
  // may draw a bar hanging off its track.
  assert.equal(windowPercent("2019-08-23T00:00:00Z", start, end), 0);
  assert.equal(windowPercent("2027-01-01T00:00:00Z", start, end), 100);

  // A window with no width is a sweep that returns immediately, which is
  // finished rather than not started.
  assert.equal(windowPercent(start, start, start), 100);
  assert.equal(windowPercent(start, end, start), 100);
  // Nothing parseable: draw nothing rather than NaN.
  assert.equal(windowPercent(undefined, start, end), 0);
});

test("the 牌谱屋 sync exposes both edges of its window, and the backend's clock", async () => {
  const [source, api, service] = await Promise.all([
    readFile(new URL("../components/paipuya-sync.tsx", import.meta.url), "utf8"),
    readFile(new URL("../lib/mjai-api.ts", import.meta.url), "utf8"),
    readFile(new URL("../../src/paipuya.rs", import.meta.url), "utf8"),
  ]);
  assert.match(api, /sync_until: string \| null;/);
  assert.match(source, /同步开始时间/);
  assert.match(source, /同步结束时间/);
  // The bars are drawn against the backend's clock, never the browser's: the
  // cursors are stamped against it, and `react-hooks/purity` refuses a
  // `Date.now()` in render anyway.
  assert.match(source, /status\?\.window_end \?\? status\?\.now/);
  assert.doesNotMatch(source, /Date\.now\(\)/);
  // Progress comes from PostgreSQL rather than this run's counters, so a
  // stopped deployment still says how far it got.
  assert.match(service, /catalog\.paipuya_cursors\(\)/);
  assert.match(service, /fn sweep_from/);
  assert.match(source, /role="progressbar"/);

  // A cursor past the right edge clamps to a full bar, and a full bar reads as
  // finished — which is the one thing it is not: that mode fetches nothing at
  // all until it is rewound. Both halves have to say so.
  assert.match(source, /const beyond = Date\.parse\(mode\.next_from\) > windowEnd/);
  assert.match(source, /在窗口之后/);
  assert.match(service, /已经晚于设定的结束时间/);
  // Rewinding keeps the totals and stops the sweep first: a worker holds its
  // position in a local and writes it back after the next page, so a rewind
  // under a running sweep half-applies.
  assert.match(service, /self\.stop\(\)\.await/);
  assert.match(
    await readFile(new URL("../../src/catalog.rs", import.meta.url), "utf8"),
    /UPDATE paipuya_cursor SET next_from/,
  );
});

/**
 * Every service's log lines have to land on some page.
 *
 * The backend keeps one 500-entry ring for all of them, tagged by source, and
 * each page filters it by prefix. That split is what stops a re-fetch sweep's
 * eighty-odd sessions from burying the collectors' lines — and it is also how a
 * service quietly becomes unreadable outside `docker logs`, which matters most
 * for the ones an error message tells the operator to go and read.
 */
test("every service's log lines land on a console page", async () => {
  const services = await Promise.all(
    ["backfill.rs", "paipuya.rs", "refetch_service.rs", "register_service.rs"].map(
      (file) => readFile(new URL(`../../src/${file}`, import.meta.url), "utf8"),
    ),
  );
  const sources = services.map((rust) => {
    const found = rust.match(/const LOG_SOURCE: &str = "([^"]+)"/);
    assert.ok(found, "every service tags its lines with a LOG_SOURCE constant");
    return found[1];
  });
  // The collectors' three, which are formatted rather than declared.
  sources.push("service", "collector:live", "module:curl-chrome");

  const pages = await Promise.all(
    ["accounts", "refetch", "watch"].map((page) =>
      readFile(new URL(`../app/(dashboard)/${page}/page.tsx`, import.meta.url), "utf8"),
    ),
  );
  const prefixes = pages.flatMap((page) => {
    const panels = page.match(/<WatchLogPanel\b[\s\S]*?\/>/g) ?? [];
    return panels.flatMap((panel) => {
      const prop = panel.match(/source=(?:\{\[([^\]]*)\]\}|"([^"]*)")/);
      // An unfiltered panel shows every service, which on a shared ring means
      // the loudest one. Adding a page must not silently reintroduce that.
      assert.ok(prop, `a log panel with no source shows every service:\n${panel}`);
      return prop[1] === undefined
        ? [prop[2]]
        : [...prop[1].matchAll(/"([^"]*)"/g)].map((match) => match[1]);
    });
  });
  assert.ok(prefixes.length > 0);

  for (const source of sources) {
    assert.ok(
      prefixes.some((prefix) => source.startsWith(prefix)),
      `no console page shows "${source}" lines; prefixes are ${prefixes.join(", ")}`,
    );
  }

  // The bar is drawn for both kinds of work now, and from rows walked rather
  // than rows fetched: on the 牌谱屋 sweep almost every uuid ahead of the cursor
  // is already held, so a bar over `replaced` reads 0.0% for months.
  const panel = await readFile(
    new URL("../components/refetch-panel.tsx", import.meta.url),
    "utf8",
  );
  assert.match(panel, /const walked = progress\?\.scanned \?\? 0;/);
  assert.match(panel, /backlog && backlog > 0\s*\?\s*Math\.min\(100, \(walked \/ backlog\) \* 100\)/);
  assert.doesNotMatch(panel, /!sweeping && backlog/);
  assert.match(panel, /role="progressbar"/);
  // And the sweep's denominator is the rows left ahead of its cursor, not the
  // catalogue's size.
  assert.match(
    await readFile(new URL("../../src/refetch_service.rs", import.meta.url), "utf8"),
    /game_uuids_ahead\(resuming\.as_ref\(\)\)/,
  );
});

/**
 * The proxy is a pool, and three separate things have to stay true for that to
 * mean anything.
 *
 * Nodes are picked on whether they can reach Mahjong Soul, not whether they can
 * reach the internet — every node this deployment ever handed to an account was
 * chosen on the strength of a `gstatic.com/generate_204`. Subscriptions
 * aggregate, and their links never leave the backend. And the two halves that
 * do not need an exit do not take one: live collection defaults to direct, and
 * the 牌谱屋 sweep refuses a proxy outright rather than inheriting one from the
 * environment.
 */
test("the proxy pool is measured against Mahjong Soul and shared by subscription", async () => {
  const [mihomo, paipuya, watch, card, pool, api] = await Promise.all(
    [
      "../../src/mihomo.rs",
      "../../src/paipuya.rs",
      "../../src/watch_service.rs",
      "../components/watch/proxy-card.tsx",
      "../components/account-pool.tsx",
      "../lib/mjai-api.ts",
    ].map((file) => readFile(new URL(file, import.meta.url), "utf8")),
  );

  // The health check the whole pool is filtered on. Asserted on the constant
  // rather than on the absence of the old endpoint: both the doc comment that
  // explains the change and the Rust test that pins it out of the generated
  // config name the URL it replaced.
  assert.match(mihomo, /HEALTH_URL: &str = "https:\/\/game\.maj-soul\.com\//);
  assert.doesNotMatch(mihomo, /HEALTH_URL: &str = "https:\/\/www\.gstatic/);
  // `lazy` skips the check for a provider nothing is using, and the pool picks
  // what to use *from* the check — an unprobed node would stay unprobed.
  assert.match(mihomo, /HEALTH_LAZY: bool = false/);
  // Two providers both offering 「香港 01」 collapse into one entry in mihomo's
  // proxy map, so a group selecting that name reaches whichever won.
  assert.match(mihomo, /additional-prefix/);

  // 牌谱屋 rate-limits by API key and has never cared where a request comes
  // from. `no_proxy` rather than simply not setting one: reqwest reads
  // HTTPS_PROXY from the environment by default.
  assert.match(paipuya, /\.no_proxy\(\)/);
  // Live collection works from the host's own address; the pool spends the
  // exits on Mahjong Soul by the thousand.
  assert.match(watch, /proxy_mode: WatchProxyMode::Direct/);

  // The console shows the host and the node counts. Never the link — that is
  // the whole of the operator's account with the provider.
  assert.match(api, /subscriptions: SubscriptionStatus\[\];/);
  assert.doesNotMatch(api, /subscription_host/);
  assert.match(card, /subscription\.host/);
  assert.match(card, /remove_subscription/);
  assert.doesNotMatch(card, /subscription\.url/);
  // `null` is not a softer yes: a node nobody has probed is not one to find out
  // about with a real account.
  assert.match(card, /node\.alive === null/);

  // Ninety accounts is ninety dropdowns, which in practice meant most of them
  // stayed on one address.
  assert.match(pool, /\/api\/accounts\/nodes/);
  assert.match(pool, /重新分配节点/);
  assert.match(pool, /href="\/api\/accounts\/export"/);

  // Both new routes forward the session, because the backend refuses either
  // without an administrator's.
  for (const route of ["app/api/accounts/nodes", "app/api/accounts/export"]) {
    const source = await readFile(
      new URL(`../${route}/route.ts`, import.meta.url),
      "utf8",
    );
    assert.match(source, /x-mjai-user-session/);
    assert.match(source, /getSessionUser/);
  }
});

/**
 * The mailbox source is probed before a batch spends anything on it.
 *
 * Four layers have to agree for this to work at all, and three of them fail
 * silently if they do not: the module has to answer `mail_probe`, the backend
 * has to ask before the first account, the console route has to be willing to
 * forward the path, and the form has to have something to press. A missing link
 * looks exactly like a probe that passed.
 *
 * It is worth pinning because of what the absence cost: the nodes have had a
 * probe since v0.40.0 and the mailboxes never did, and a dead mailbox source is
 * more expensive than a dead node. A node costs a login; a mailbox costs an
 * account — the signup has already happened by the time the code is polled for,
 * so the address is spent and Mahjong Soul holds an account this side cannot
 * confirm.
 */
test("the mailbox source is probed before a run spends an account on it", async () => {
  const [module, register, api, form, route] = await Promise.all(
    [
      "../../modules/register/curl-chrome/module.py",
      "../../src/register_service.rs",
      "../../src/api.rs",
      "../components/account-register.tsx",
      "../app/api/accounts/register/[[...path]]/route.ts",
    ].map((file) => readFile(new URL(file, import.meta.url), "utf8")),
  );

  // The module answers it, and reaches the inbox by the same call the code
  // poll uses — a probe down a different path can pass while fetching cannot.
  assert.match(module, /if method == "mail_probe"/);
  assert.match(module, /async def mailbox_inbox\(/);

  // Asked before the batch, not per account.
  assert.match(register, /self\.probe_mail\(&worker, &request\)\.await\?;/);
  // And every secret the probe could put in an error is redacted first: the
  // mailbox credential travels as a query parameter, so a transport error that
  // quotes its own URL quotes the credential.
  assert.match(register, /fn register_mail_secrets/);
  assert.match(register, /self\.logs\.register_secret\(temp\.api_key\.trim\(\)\)/);
  assert.match(register, /self\.logs\.register_secret\(&cloud\.admin_password\)/);

  // The button, and the route that lets it through. In the session-only group
  // on both sides: the body carries the same credentials a run does.
  assert.match(api, /"\/api\/v1\/accounts\/register\/probe"/);
  assert.match(route, /new Set\(\[.*"probe".*\]\)/);
  assert.match(form, /\/api\/accounts\/register\/probe/);
  assert.match(form, /测试邮箱/);
});
