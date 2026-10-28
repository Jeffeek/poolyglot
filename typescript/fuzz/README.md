# connpool fuzz (TypeScript)

Property-based harness for the async primitives in `src/pools/async-primitives.ts` —
`AsyncCondition` + `awaitShielded` with `AbortSignal` cancellation. It generates randomized
lock/wait/notify/abort scenarios and asserts mutual exclusion, no lost wakeups, and shielded work
always completing. It is not part of the test suite or coverage gate.

Powered by [fast-check](https://fast-check.dev), a **dev-only** dependency — it's a `devDependency`
and the published package only ships `dist/` (see `package.json` `files`), so it never reaches
consumers of `@jeffeek/connpool`. No build step, no native dependencies.

## Requirements

Node 22+. `fast-check` and `tsx` are already dev dependencies (`npm install`).

## Run

```bash
npm run fuzz                                       # 200 runs (~35s)
npm run fuzz -- --num-runs=20000 --seed=42         # longer soak, fixed seed
npm run fuzz -- --max-seconds=120                  # run for a wall-clock budget instead
```

A concrete seed is always chosen and printed, so any run reproduces exactly via `--seed=<n>`, and
progress is logged every couple of seconds (`... N runs ok`) so a long run never looks hung. On a
failure fast-check **shrinks** to a minimal counterexample and prints it alongside the seed.

The default run count is modest on purpose: some scenarios wait on a notify that never arrives and
only settle at the 1s watchdog (~170ms/run average), so CI drives the volume via `--max-seconds`.

A failure is any asserted invariant breaking — a mutual-exclusion violation, a hang caught by the
per-call watchdog, an unexpected `wait()` rejection, or an unhandled promise rejection — all of
which `throw`, which fast-check reports as a failed property (non-zero exit) with the shrunk
scenario and the reproducing seed.
