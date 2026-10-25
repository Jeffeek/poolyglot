import { defineConfig } from "tsup";

// Dual build: ESM (dist/index.js) + CJS (dist/index.cjs), each with its own type
// declarations (index.d.ts / index.d.cts) and source maps. The library source is
// authored as ESM with NodeNext `.js` import specifiers; esbuild resolves those to the
// `.ts` sources. Type-checking is done separately (`npm run typecheck`); tsup/esbuild
// only transpiles + emits.
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "es2022",
  outDir: "dist",
  treeshake: true,
});
