// Builds the async-primitives fuzz target to a self-contained CommonJS bundle, then runs jsfuzz
// against it. Why a build step: jsfuzz's worker loads the target via CommonJS `require()` and
// instruments it with istanbul for *coverage-guided* mutation — that's its whole value over a
// blind random fuzzer. Running the target through a tsx loader instead hid the real source from
// istanbul (coverage stayed at 0), degrading jsfuzz to random mutation. Bundling the TypeScript
// target + its `../src/pools/async-primitives.js` import into one plain `.cjs` lets istanbul
// instrument the real logic, so coverage guidance works. No dist import (AsyncCondition /
// awaitShielded aren't in the public barrel). See fuzz/async-primitives.fuzz.ts for the harness.
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { build } from "esbuild";

const require = createRequire(import.meta.url);
const jsfuzzBin = require.resolve("jsfuzz/build/src/index.js");

const target = "fuzz/build/async-primitives.fuzz.cjs";
mkdirSync("fuzz/build", { recursive: true });
mkdirSync("fuzz/corpus", { recursive: true }); // gitignored; empty on a fresh checkout
await build({
  entryPoints: ["fuzz/async-primitives.fuzz.ts"],
  outfile: target,
  bundle: true, // inline async-primitives so istanbul instruments the real logic
  platform: "node",
  format: "cjs",
  target: "node22",
  sourcemap: "inline",
});

const extraArgs = process.argv.slice(2);
execFileSync(process.execPath, [jsfuzzBin, target, "fuzz/corpus", ...extraArgs], {
  stdio: "inherit",
});
