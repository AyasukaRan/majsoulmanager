/**
 * The twelve `{players}p-{room}-{length}` tokens the indexer derives, in
 * Chinese. Shared rather than copied: the record filter offers these as its
 * dropdown and the trends page names them in its breakdown, and two copies
 * would drift the day Majsoul adds a room.
 */
export const RULE_LABELS: Record<string, string> = {
  "4p-gold-east": "金之间·东风",
  "4p-gold-south": "金之间·东南",
  "4p-jade-east": "玉之间·东风",
  "4p-jade-south": "玉之间·东南",
  "4p-throne-east": "王座之间·东风",
  "4p-throne-south": "王座之间·东南",
  "3p-gold-east": "三麻金·东风",
  "3p-gold-south": "三麻金·东南",
  "3p-jade-east": "三麻玉·东风",
  "3p-jade-south": "三麻玉·东南",
  "3p-throne-east": "三麻王座·东风",
  "3p-throne-south": "三麻王座·东南",
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
