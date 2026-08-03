/**
 * The twelve `{players}p-{room}-{length}` tokens the indexer derives, in
 * Chinese. Shared rather than copied: the record filter offers these as its
 * dropdown and the trends page names them in its breakdown, and two copies
 * would drift the day Majsoul adds a room.
 *
 * Every label is the three `RULE_FACETS` labels joined, in token order, so a
 * row of the breakdown reads as exactly the three filter chips that would
 * select it. The first version named the player count only for 三麻 and dropped
 * 之间 only there too, which made half the list look like a different naming
 * scheme from the other half.
 */
export const RULE_LABELS: Record<string, string> = {
  "4p-gold-east": "四麻·金之间·东风",
  "4p-gold-south": "四麻·金之间·东南",
  "4p-jade-east": "四麻·玉之间·东风",
  "4p-jade-south": "四麻·玉之间·东南",
  "4p-throne-east": "四麻·王座之间·东风",
  "4p-throne-south": "四麻·王座之间·东南",
  "3p-gold-east": "三麻·金之间·东风",
  "3p-gold-south": "三麻·金之间·东南",
  "3p-jade-east": "三麻·玉之间·东风",
  "3p-jade-south": "三麻·玉之间·东南",
  "3p-throne-east": "三麻·王座之间·东风",
  "3p-throne-south": "三麻·王座之间·东南",
};

/**
 * An unrecognised token is shown as itself rather than as a dash — a
 * thirteenth mode has to stay legible — and the empty one is named, because
 * the index stores "" where the converter left no mode on the record and a
 * blank row in a breakdown reads as a rendering bug.
 */
export function ruleLabel(rule: string) {
  return RULE_LABELS[rule] ?? (rule || "未知场次");
}

/**
 * The three parts a rule token is made of, which are also the three questions
 * worth asking of it separately: how many players, which room, how long. The
 * `value`s are the exact substrings the API parses, and it rejects anything
 * else — so this list and `RulePlayers`/`RuleRoom`/`RuleLength` in
 * `src/catalog.rs` have to say the same thing.
 */
export const RULE_FACETS = [
  {
    key: "players" as const,
    label: "人数",
    options: [
      { value: "4p", label: "四麻" },
      { value: "3p", label: "三麻" },
    ],
  },
  {
    key: "room" as const,
    label: "房间",
    options: [
      { value: "gold", label: "金之间" },
      { value: "jade", label: "玉之间" },
      { value: "throne", label: "王座之间" },
    ],
  },
  {
    key: "length" as const,
    label: "场长",
    options: [
      { value: "east", label: "东风" },
      { value: "south", label: "东南" },
    ],
  },
];

export type RuleFacetKey = (typeof RULE_FACETS)[number]["key"];

/** Every facet unset, which the API reads as "no predicate at all". */
export const NO_RULE_FILTER: Record<RuleFacetKey, string[]> = {
  players: [],
  room: [],
  length: [],
};
