// Mirror the ESM declaration tree (*.d.ts, emitted by `tsc -p tsconfig.build.json`) to a
// CJS one (*.d.cts) for the package's `require` types condition.
//
// Why a script and not tsup: TypeScript 7's native compiler ships no JS compiler API, so
// tsup's rollup-plugin-dts can no longer roll up declarations. We emit a plain tree with
// tsc instead and mirror it here. Relative `.js` specifiers are rewritten to `.cjs` so that
// CJS-mode type resolution (`.cjs` -> `.d.cts`) stays inside the .d.cts tree and never falls
// back into the ESM-mode `.d.ts` files (the "masquerading as ESM" hazard).
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const DIST = "dist";

// A relative specifier in quotes ending in `.js` -> same path ending in `.cjs`.
const RELATIVE_JS = /(['"]\.\.?\/[^'"]*?)\.js(['"])/g;

function convertDts(path) {
  const out = path.slice(0, -".d.ts".length) + ".d.cts";
  const src = readFileSync(path, "utf8")
    .replace(RELATIVE_JS, "$1.cjs$2")
    .replace(/(\/\/# sourceMappingURL=.+)\.d\.ts\.map/g, "$1.d.cts.map");
  writeFileSync(out, src);
}

function convertMap(path) {
  const out = path.slice(0, -".d.ts.map".length) + ".d.cts.map";
  const map = JSON.parse(readFileSync(path, "utf8"));
  if (typeof map.file === "string") map.file = map.file.replace(/\.d\.ts$/, ".d.cts");
  writeFileSync(out, JSON.stringify(map));
}

function walk(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) walk(p);
    else if (entry.name.endsWith(".d.ts.map")) convertMap(p);
    else if (entry.name.endsWith(".d.ts")) convertDts(p);
  }
}

walk(DIST);
