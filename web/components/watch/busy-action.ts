"use client";

import { useCallback, useState } from "react";

/**
 * What an action reports when it succeeds. Returning nothing leaves the message
 * line as the action itself left it, which module installation needs: it
 * reports before it reloads the lists, and a reload that fails has to be able
 * to say so without being overwritten by a success that already happened.
 */
export type RunAction = (
  fallback: string,
  action: () => Promise<string | void>,
) => Promise<void>;

/**
 * The settings page writes one configuration, so its actions share one busy
 * flag and one message line: while any of them is in flight none of the others
 * may fire, and what the operator has to read is the last thing that happened,
 * whichever card it happened in.
 */
export function useBusyAction() {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const run = useCallback<RunAction>(async (fallback, action) => {
    setBusy(true);
    setMessage(null);
    try {
      const reported = await action();
      if (typeof reported === "string") setMessage(reported);
    } catch (error) {
      // The backend's own wording is what explains a rejected configuration, so
      // the fallback is only for a failure that carries no message at all.
      setMessage(error instanceof Error ? error.message : fallback);
    } finally {
      setBusy(false);
    }
  }, []);

  return { busy, message, setMessage, run };
}
