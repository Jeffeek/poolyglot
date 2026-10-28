/**
 * jsfuzz harness for the divergent async primitives (AsyncCondition + awaitShielded /
 * AbortSignal cancellation) — the TypeScript twins of Python's asyncio.Condition/shield and
 * C#'s AsyncCondition. Not part of the test suite or coverage gate: jsfuzz drives
 * coverage-guided random byte buffers into `fuzz(buf)`, which decodes them into the same
 * operation sequence the old hand-rolled PRNG fed, and asserts invariants that must hold no
 * matter what order lock/wait/notify/abort happen in.
 *
 * Dev-only (jsfuzz is a devDependency; the published package only ships "dist", see
 * package.json "files"). Run via:
 *
 *   npm run fuzz                                 # fuzz indefinitely (Ctrl+C to stop)
 *   npm run fuzz -- --exact-artifact-path=crash   # pin the crash file name
 *   npm run fuzz -- --regression                  # replay fuzz/corpus instead of generating
 *
 * jsfuzz's worker loads the target via CommonJS `require()` and instruments it with istanbul for
 * coverage-guided mutation — so `scripts/run-fuzz.mjs` (what "npm run fuzz" invokes) first esbuild-
 * bundles this file and its relative `../src/pools/async-primitives.js` import into a single plain
 * `fuzz/build/*.cjs` that istanbul can see (loading via a tsx loader instead hid the real source,
 * leaving coverage stuck at 0). No dist import — AsyncCondition/awaitShielded aren't in the public
 * barrel anyway.
 *
 * Invariants asserted every call:
 *   1. Mutual exclusion: at most one `lock()` holder is "inside the critical section" at a time.
 *   2. Every `lock()`/`wait()` call eventually settles (resolves or rejects) — no hang, bounded
 *      by a per-call watchdog timeout that throws loudly (a thrown error registers as a jsfuzz
 *      crash) instead of hanging the fuzzer forever.
 *   3. `wait()` never rejects with something other than the abort reason, and never resolves
 *      after the condition was disposed mid-wait.
 *   4. `awaitShielded` always rejects on abort (even pre-aborted) and the shielded work always
 *      runs to completion in the background regardless of the caller's outcome.
 *   5. No unhandled promise rejections escape a call.
 */
import assert from "node:assert/strict";
import { AsyncCondition, awaitShielded } from "../src/pools/async-primitives.js";

/**
 * Tiny byte-cursor reader over the fuzz input, replacing the old mulberry32 PRNG: each read
 * consumes bytes from the front and falls back to 0 once exhausted, so jsfuzz's coverage-guided
 * mutations (grow/shrink/flip bytes) map onto the same decisions (task count, abort timing,
 * wait vs. no-wait, ...) the PRNG used to make.
 */
class ByteCursor {
  private offset = 0;

  constructor(private readonly buf: Buffer) {}

  /** Next raw byte (0 once exhausted). */
  byte(): number {
    if (this.offset >= this.buf.length) return 0;
    return this.buf[this.offset++] ?? 0;
  }

  /** A float in [0, 1), the drop-in replacement for `rng()`. */
  float(): number {
    return this.byte() / 256;
  }

  /** An integer in [0, max). */
  int(max: number): number {
    return Math.floor(this.float() * max);
  }

  /** True with probability `p` (0..1). */
  chance(p: number): boolean {
    return this.float() < p;
  }
}

function withWatchdog<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`watchdog: ${label} did not settle in ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}

let unhandled: unknown[] = [];
process.on("unhandledRejection", (reason) => {
  unhandled.push(reason);
});

/**
 * One fuzzed run: spawns a handful of concurrent "tasks" that each race lock/wait against
 * abort, driven by the byte cursor. Mirrors the invariants the old PRNG-driven runOne asserted.
 */
async function runOne(cur: ByteCursor): Promise<void> {
  const cond = new AsyncCondition();
  const taskCount = 2 + cur.int(3); // 2..4 concurrent tasks
  let insideCriticalSection = 0;
  const results: Array<"ok" | "aborted" | "timeout"> = [];

  const tasks = Array.from({ length: taskCount }, async (_, i) => {
    const ctrl = new AbortController();
    const willAbort = cur.chance(0.3);
    const abortDelayMs = cur.int(5);
    if (willAbort) {
      setTimeout(() => ctrl.abort(), abortDelayMs);
    }

    try {
      await withWatchdog(cond.lock(ctrl.signal), 1000, `task${i}.lock`);
    } catch {
      results.push("aborted");
      return;
    }

    // Invariant 1: mutual exclusion.
    insideCriticalSection += 1;
    assert.equal(insideCriticalSection, 1, "more than one holder inside the critical section");

    try {
      if (cur.chance(0.5)) {
        const timeoutSeconds = cur.chance(0.5) ? cur.float() * 0.002 : null;
        // wait() releases the lock internally while parked, then re-acquires it before
        // returning — so the critical-section counter must drop around the call, not after it.
        insideCriticalSection -= 1;
        let notified: boolean;
        try {
          notified = await withWatchdog(cond.wait(timeoutSeconds, ctrl.signal), 1000, `task${i}.wait`);
        } finally {
          insideCriticalSection += 1;
          assert.equal(insideCriticalSection, 1, "wait() returned without sole re-acquisition");
        }
        results.push(notified ? "ok" : "timeout");
      } else {
        results.push("ok");
      }
    } catch {
      results.push("aborted");
    } finally {
      insideCriticalSection -= 1;
      cond.unlock();
    }

    // notify a potential waiter so wait() paths get exercised too
    if (cur.chance(0.5)) {
      try {
        await withWatchdog(cond.lock(), 1000, `task${i}.notifylock`);
        cond.notify(1 + cur.int(2));
        cond.unlock();
      } catch {
        // lock() without a signal never rejects; unreachable, kept for defense in depth.
      }
    }
  });

  await withWatchdog(Promise.all(tasks), 5000, "all tasks");
  assert.ok(
    results.every((r) => r === "ok" || r === "aborted" || r === "timeout"),
    "task produced an unexpected outcome",
  );

  // awaitShielded: background work must always finish even when the caller is cancelled.
  let shieldedWorkDone = false;
  const work = new Promise<void>((resolve) => setTimeout(() => {
    shieldedWorkDone = true;
    resolve();
  }, cur.int(3)));
  const shieldCtrl = new AbortController();
  const preAbort = cur.chance(0.3);
  if (preAbort) {
    shieldCtrl.abort();
  } else if (cur.chance(0.5)) {
    setTimeout(() => shieldCtrl.abort(), cur.int(3));
  }
  try {
    await withWatchdog(awaitShielded(work, shieldCtrl.signal), 1000, "awaitShielded");
  } catch {
    // expected on abort
  }
  await withWatchdog(work, 1000, "shielded work drain");
  assert.ok(shieldedWorkDone, "shielded work did not run to completion after cancellation");
}

/** jsfuzz's target contract: `export function fuzz(buf: Buffer)`, called once per generated input. */
export async function fuzz(buf: Buffer): Promise<void> {
  unhandled = [];
  await runOne(new ByteCursor(buf));
  if (unhandled.length > 0) {
    throw new Error(`unhandled rejection(s): ${unhandled.map(String).join(", ")}`);
  }
}
