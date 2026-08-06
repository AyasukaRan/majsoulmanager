/**
 * Reads the account registrar's `accounts.jsonl` into pool rows.
 *
 * One line per account, as the registrar writes it:
 * `{"email": …, "password": …, "nickname": …, "account_id": …, "ts": …}`.
 * Only the first three are of any use here; a line missing either the address
 * or the password is counted rather than guessed at.
 *
 * What it refuses is what the backend refuses, because the backend refuses the
 * *document*: one bad row in `AccountDocument::validate` is a 400 for all fifty,
 * and the operator is then looking for it among fifty boxes. So a duplicate —
 * compared case-insensitively, the way `src/accounts.rs` compares e-mail logins
 * — and a login carrying whitespace or a `/` are dropped here, where they can be
 * counted, rather than staged for a save that cannot succeed.
 *
 * Plain JS, not TS, so `node --test` can import it without a build step. It is
 * the only piece of this import worth a real assertion, and asserting it
 * against the source text would not fail if the logic broke.
 */

/**
 * @typedef {{ username: string, password: string, note: string }} ImportedAccount
 * @typedef {{ accounts: ImportedAccount[], duplicates: number, unusable: number }} ImportReport
 */

/**
 * @param {string} text the file, verbatim
 * @param {Iterable<string>} taken usernames already in the pool
 * @returns {ImportReport}
 */
export function parseAccountsJsonl(text, taken) {
  const seen = new Set(
    [...taken].map((username) => username.trim().toLowerCase()),
  );
  /** @type {ImportedAccount[]} */
  const accounts = [];
  let duplicates = 0;
  let unusable = 0;

  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    /** @type {unknown} */
    let row;
    try {
      row = JSON.parse(trimmed);
    } catch {
      unusable += 1;
      continue;
    }
    if (typeof row !== "object" || row === null) {
      unusable += 1;
      continue;
    }

    const record = /** @type {Record<string, unknown>} */ (row);
    const login = record.email ?? record.username;
    const password = record.password;
    if (
      typeof login !== "string" ||
      typeof password !== "string" ||
      !password.trim() ||
      // Trimmed first, the way the backend trims it, so surrounding whitespace
      // is not what makes an otherwise fine address unusable.
      !login.trim() ||
      /[\s/]/.test(login.trim())
    ) {
      unusable += 1;
      continue;
    }

    const username = login.trim();
    // Within the file as well as against the pool: the registrar appends, and a
    // resumed run that retried an address writes it twice.
    if (seen.has(username.toLowerCase())) {
      duplicates += 1;
      continue;
    }
    seen.add(username.toLowerCase());
    accounts.push({
      username,
      // Untrimmed: what the registrar generated is what logs in.
      password,
      // The in-game name is the only way to tell two of these apart on a
      // scoreboard, and the note column is where an operator would write it.
      note: typeof record.nickname === "string" ? record.nickname : "",
    });
  }

  return { accounts, duplicates, unusable };
}
