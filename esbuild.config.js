import * as esbuild from "esbuild";

const sharedConfig = {
  entryPoints: ["src/index.ts"],
  bundle: true,
  minify: true,
  platform: "node",
  target: "node18",
  external: ["@aws-sdk/client-glue", "@aws-sdk/client-s3", "@duckdb/node-api"],
};

// ESM build
await esbuild.build({
  ...sharedConfig,
  format: "esm",
  outfile: "dist/esm/index.js",
});

// CJS build
await esbuild.build({
  ...sharedConfig,
  format: "cjs",
  outfile: "dist/cjs/index.js",
});
