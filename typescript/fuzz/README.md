# connpool fuzz (TypeScript)

Harness for the async primitives in `src/pools/async-primitives.ts` — `AsyncCondition` +
`awaitShielded` with `AbortSignal` cancellation. It decodes coverage-guided random byte buffers into
lock/wait/notify/abort operation sequences and asserts mutual exclusion, no lost wakeups, and
shielded work always completing. It is not part of the test suite or coverage gate.

Powered by [jsfuzz](https://gitlab.com/gitlab-org/security-products/analyzers/fuzzers/jsfuzz), a
**dev-only** dependency — it's a `devDependency` and the published package only ships `dist/`
(see `package.json` `files`), so it never reaches consumers of `@jeffeek/connpool`.

## Requirements

Node 22+. `jsfuzz` and `esbuild` are already dev dependencies (`npm install`).

> **Windows note:** jsfuzz's `pidusage` dependency shells out to `wmic` to read memory, which
> Windows 11 removed — so `npm run fuzz` crashes after a few seconds. Run under WSL/Linux.

## Run

```bash
npm run fuzz                                 # fuzz indefinitely, writing new corpus entries as it finds coverage (Ctrl+C to stop)
npm run fuzz -- --regression                 # replay fuzz/corpus instead of generating new inputs
npm run fuzz -- --exact-artifact-path=crash  # write a crash to a fixed path instead of crash-<hash>
npm run fuzz -- --timeout=10                 # fail a single fuzz() call that runs longer than N seconds (default 30)
```

jsfuzz loads the fuzz target via CommonJS `require()` in a forked worker and instruments it with
istanbul for **coverage-guided** mutation. So `npm run fuzz` runs `scripts/run-fuzz.mjs`, which
first esbuild-bundles `async-primitives.fuzz.ts` (and its relative `../src/pools/async-primitives.js`
import) into a single plain `fuzz/build/*.cjs`, then points jsfuzz at that. Bundling to CJS — rather
than loading the TypeScript through a `tsx` loader — is what lets istanbul see the real source; under
a loader, coverage stayed stuck at 0 and jsfuzz degraded to blind random mutation. No dependency on
the public `dist` barrel (which doesn't export `AsyncCondition`/`awaitShielded` anyway).

Corpus entries accumulate under `fuzz/corpus/` (gitignored — regenerate locally, nothing to
commit) as jsfuzz discovers inputs that hit new coverage.

A failure is any asserted invariant breaking — a mutual-exclusion violation, a hang caught by the
per-call watchdog, an unexpected `wait()` rejection, or an unhandled promise rejection — all of
which `throw`, which jsfuzz reports as a crash (exit code 1, crash buffer written to disk, hex
dump under ~200 bytes printed to the console for quick reproduction).
