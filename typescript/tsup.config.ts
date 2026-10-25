import { defineConfig } from "tsup";

// Dual build: ESM (dist/index.js) + CJS (dist/index.cjs) with source maps. The library
// source is authored as ESM with NodeNext `.js` import specifiers; esbuild resolves those
// to the `.ts` sources. Type-checking is done separately (`npm run typecheck`); tsup/esbuild
// only transpiles + emits the JS.
//
// Declarations are emitted separately (`tsc -p tsconfig.build.json` + scripts/make-cts-dts.mjs),
// not by tsup: TypeScript 7's native compiler ships no JS compiler API, so tsup's
// rollup-plugin-dts can no longer roll up declarations. See package.json "build".
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: false,
  sourcemap: true,
  clean: true,
  target: "es2022",
  outDir: "dist",
  treeshake: true,
});
