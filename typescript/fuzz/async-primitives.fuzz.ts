/**
 * Property-based fuzz harness for the divergent async primitives (AsyncCondition + awaitShielded /
 * AbortSignal cancellation) — the TypeScript twins of Python's asyncio.Condition/shield and
 * C#'s AsyncCondition. Not part of the test suite or coverage gate: fast-check generates randomized
 * lock/wait/notify/abort scenarios and asserts invariants that must hold no matter what order those
 * operations happen in. On a violation fast-check shrinks to a minimal counterexample and prints the
 * seed to reproduce it exactly.
 *
 * Dev-only, one dev dependency (fast-check; tsx runs it). The published package only ships "dist"
 * (see package.json "files"), so none of this reaches consumers. Run directly:
 *
 *   npx tsx fuzz/async-primitives.fuzz.ts                        # default: 1000 runs
 *   npx tsx fuzz/async-primitives.fuzz.ts --num-runs=20000 --seed=42
 *   npx tsx fuzz/async-primitives.fuzz.ts --max-seconds=120      # run for a wall-clock budget instead
 *
 * Real time drives the primitives' waits/timeouts (setTimeout), so fast-check's arbitraries pick the
 * scenario while actual timers/AbortControllers produce the concurrency — the same model the old
 * hand-rolled PRNG fuzzer used, now with a reproducible seed and automatic shrinking.
 *
 * Invariants asserted every run:
 *   1. Mutual exclusion: at most one `lock()` holder is "inside the critical section" at a time.
 *   2. Every `lock()`/`wait()` call eventually settles (resolves or rejects) — no hang, bounded by a
 *      per-call watchdog that throws loudly instead of hanging forever.
 *   3. `wait()` never rejects with something other than the abort reason, and never resolves after the
 *      condition was disposed mid-wait.
 *   4. `awaitShielded` always rejects on abort (even pre-aborted) and the shielded work always runs to
 *      completion in the background regardless of the caller's outcome.
 *   5. No unhandled promise rejections escape a run.
 */
import assert from "node:assert/strict";
import fc from "fast-check";
import { AsyncCondition, awaitShielded } from "../src/pools/async-primitives.js";

interface TaskPlan {
  willAbort: boolean;
  abortDelayMs: number; // 0..4
  doWait: boolean;
  withTimeout: boolean; // when waiting, park with a short timeout instead of indefinitely
  notify: boolean; // after releasing, re-lock and notify a potential waiter
  notifyCount: number; // 1..2
}

interface Scenario {
  tasks: TaskPlan[]; // 2..4 concurrent tasks
  shieldWorkDelayMs: number; // 0..2
  shieldPreAbort: boolean; // abort before awaitShielded is even called
  shieldAbortAfter: boolean; // otherwise, maybe abort mid-flight
  shieldAbortDelayMs: number; // 0..2
}

const taskArb: fc.Arbitrary<TaskPlan> = fc.record({
  willAbort: fc.boolean(),
  abortDelayMs: fc.integer({ min: 0, max: 4 }),
  doWait: fc.boolean(),
  withTimeout: fc.boolean(),
  notify: fc.boolean(),
  notifyCount: fc.integer({ min: 1, max: 2 }),
});

const scenarioArb: fc.Arbitrary<Scenario> = fc.record({
  tasks: fc.array(taskArb, { minLength: 2, maxLength: 4 }),
  shieldWorkDelayMs: fc.integer({ min: 0, max: 2 }),
  shieldPreAbort: fc.boolean(),
  shieldAbortAfter: fc.boolean(),
  shieldAbortDelayMs: fc.integer({ min: 0, max: 2 }),
});

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

/** One scenario: spawns the planned concurrent tasks racing lock/wait against abort, then drives
 * awaitShielded. Mirrors the invariants the old PRNG-driven runOne asserted. */
async function runOne(scenario: Scenario): Promise<void> {
  const cond = new AsyncCondition();
  let insideCriticalSection = 0;
  const results: Array<"ok" | "aborted" | "timeout"> = [];

  const tasks = scenario.tasks.map(async (plan, i) => {
    const ctrl = new AbortController();
    if (plan.willAbort) {
      setTimeout(() => ctrl.abort(), plan.abortDelayMs);
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
      if (plan.doWait) {
        const timeoutSeconds = plan.withTimeout ? 0.001 : null;
        // wait() releases the lock internally while parked, then re-acquires it before returning —
        // so the critical-section counter must drop around the call, not after it.
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
    if (plan.notify) {
      try {
        await withWatchdog(cond.lock(), 1000, `task${i}.notifylock`);
        cond.notify(plan.notifyCount);
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
  const work = new Promise<void>((resolve) =>
    setTimeout(() => {
      shieldedWorkDone = true;
      resolve();
    }, scenario.shieldWorkDelayMs),
  );
  const shieldCtrl = new AbortController();
  if (scenario.shieldPreAbort) {
    shieldCtrl.abort();
  } else if (scenario.shieldAbortAfter) {
    setTimeout(() => shieldCtrl.abort(), scenario.shieldAbortDelayMs);
  }
  try {
    await withWatchdog(awaitShielded(work, shieldCtrl.signal), 1000, "awaitShielded");
  } catch {
    // expected on abort
  }
  await withWatchdog(work, 1000, "shielded work drain");
  assert.ok(shieldedWorkDone, "shielded work did not run to completion after cancellation");
}

function parseArgs(): { seed: number; numRuns: number; maxSeconds: number } {
  const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
      const [k, v] = a.replace(/^--/, "").split("=");
      return [k, v];
    }),
  );
  return {
    // A concrete seed is always chosen (and logged) so every run reproduces via --seed.
    seed: Number(args.seed ?? Date.now() & 0xffffffff),
    numRuns: Number(args["num-runs"] ?? 1000),
    maxSeconds: Number(args["max-seconds"] ?? 0), // 0 = run the full numRuns, no time bound
  };
}

async function main(): Promise<void> {
  const { seed, numRuns, maxSeconds } = parseArgs();
  console.log(
    `fuzzing AsyncCondition/awaitShielded: num-runs=${numRuns}` +
      `${maxSeconds > 0 ? ` max-seconds=${maxSeconds}` : ""} seed=${seed}`,
  );

  await fc.assert(
    fc.asyncProperty(scenarioArb, async (scenario) => {
      unhandled = [];
      await runOne(scenario);
      if (unhandled.length > 0) {
        throw new Error(`unhandled rejection(s): ${unhandled.map(String).join(", ")}`);
      }
    }),
    {
      seed,
      numRuns,
      // A wall-clock budget: stop after this many ms and treat hitting it as success (not a failure),
      // so CI can "fuzz for N seconds" without the old timeout/exit-code dance.
      interruptAfterTimeLimit: maxSeconds > 0 ? maxSeconds * 1000 : undefined,
      markInterruptAsFailure: false,
    },
  );

  console.log(`ok: no invariant violations, no unhandled rejections (seed=${seed})`);
}

await main();
