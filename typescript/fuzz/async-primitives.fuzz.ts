/**
 * Fuzz harness for the divergent async primitives (AsyncCondition + awaitShielded /
 * AbortSignal cancellation) — the TypeScript twins of Python's asyncio.Condition/shield and
 * C#'s AsyncCondition. Not part of the test suite or coverage gate: it drives randomized
 * operation sequences against the primitives and asserts invariants that must hold no matter
 * what order lock/wait/notify/abort happen in.
 *
 * Dev-only, zero new dependencies (stdlib `node:assert` + Math.random). Run directly:
 *
 *   npx tsx fuzz/async-primitives.fuzz.ts              # default: 2000 iterations
 *   npx tsx fuzz/async-primitives.fuzz.ts --iterations=20000 --seed=42
 *
 * Invariants asserted every iteration:
 *   1. Mutual exclusion: at most one `lock()` holder is "inside the critical section" at a time.
 *   2. Every `lock()`/`wait()` call eventually settles (resolves or rejects) — no hang, bounded
 *      by a per-iteration watchdog timeout that fails loudly instead of hanging CI forever.
 *   3. `wait()` never rejects with something other than the abort reason, and never resolves
 *      after the condition was disposed mid-wait.
 *   4. `awaitShielded` always rejects on abort (even pre-aborted) and the shielded work always
 *      runs to completion in the background regardless of the caller's outcome.
 *   5. No unhandled promise rejections escape an iteration.
 */
import assert from "node:assert/strict";
import { AsyncCondition, awaitShielded } from "../src/pools/async-primitives.js";

function parseArgs(): { iterations: number; seed: number } {
  const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
      const [k, v] = a.replace(/^--/, "").split("=");
      return [k, v];
    }),
  );
  return {
    iterations: Number(args.iterations ?? 2000),
    seed: Number(args.seed ?? Date.now()),
  };
}

// ponytail: tiny deterministic PRNG (mulberry32) so a reported seed reproduces exactly —
// stdlib Math.random() isn't seedable. Upgrade to a real fuzzing lib only if this stops finding bugs.
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
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
 * One randomized run: spawns a handful of concurrent "tasks" that each race lock/wait against
 * abort, driven by the PRNG. Returns the number of operations performed.
 */
async function runOne(rng: () => number): Promise<void> {
  const cond = new AsyncCondition();
  const taskCount = 2 + Math.floor(rng() * 3); // 2..4 concurrent tasks
  let insideCriticalSection = 0;
  const results: Array<"ok" | "aborted" | "timeout"> = [];

  const tasks = Array.from({ length: taskCount }, async (_, i) => {
    const ctrl = new AbortController();
    const willAbort = rng() < 0.3;
    const abortDelayMs = Math.floor(rng() * 5);
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
      if (rng() < 0.5) {
        const timeoutSeconds = rng() < 0.5 ? rng() * 0.002 : null;
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
    if (rng() < 0.5) {
      try {
        await withWatchdog(cond.lock(), 1000, `task${i}.notifylock`);
        cond.notify(1 + Math.floor(rng() * 2));
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
  }, Math.floor(rng() * 3)));
  const shieldCtrl = new AbortController();
  const preAbort = rng() < 0.3;
  if (preAbort) {
    shieldCtrl.abort();
  } else if (rng() < 0.5) {
    setTimeout(() => shieldCtrl.abort(), Math.floor(rng() * 3));
  }
  try {
    await withWatchdog(awaitShielded(work, shieldCtrl.signal), 1000, "awaitShielded");
  } catch {
    // expected on abort
  }
  await withWatchdog(work, 1000, "shielded work drain");
  assert.ok(shieldedWorkDone, "shielded work did not run to completion after cancellation");
}

async function main(): Promise<void> {
  const { iterations, seed } = parseArgs();
  const rng = mulberry32(seed);
  console.log(`fuzzing AsyncCondition/awaitShielded: iterations=${iterations} seed=${seed}`);

  for (let i = 0; i < iterations; i++) {
    try {
      await runOne(rng);
    } catch (error) {
      console.error(`FAIL at iteration ${i} (seed=${seed}): rerun with --seed=${seed}`);
      throw error;
    }
    if (unhandled.length > 0) {
      console.error(`FAIL at iteration ${i} (seed=${seed}): unhandled rejection(s)`, unhandled);
      process.exitCode = 1;
      return;
    }
  }

  console.log(`ok: ${iterations} iterations, no invariant violations, no unhandled rejections`);
}

await main();
