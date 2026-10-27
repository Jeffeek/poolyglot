# connpool fuzz (TypeScript)

Harness for the async primitives in `src/pools/async-primitives.ts` — `AsyncCondition` +
`awaitShielded` with `AbortSignal` cancellation. It drives randomized lock/wait/notify/abort
sequences and asserts mutual exclusion, no lost wakeups, and shielded work always completing. It is
not part of the test suite or coverage gate, and adds no dependencies (`node:assert` + `Math.random`).

## Requirements

Node 22+ and `tsx` (already a dev dependency). No special toolchain.

## Run

```bash
npx tsx fuzz/async-primitives.fuzz.ts                        # 2000 iterations
npx tsx fuzz/async-primitives.fuzz.ts --iterations=20000 --seed=42
npx tsx fuzz/async-primitives.fuzz.ts --max-seconds=120      # run for a wall-clock budget instead
```

A failure is any asserted invariant breaking: a mutual-exclusion violation, a hang caught by the
per-iteration watchdog, an unexpected `wait()` rejection, or an unhandled promise rejection.
