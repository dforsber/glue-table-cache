import * as esbuild from 'esbuild';

const baseConfig = {
  entryPoints: ['src/index.ts'],
  bundle: true,
  minify: true,
  sourcemap: true,
  target: ['es2020'],
  platform: 'neutral',
  format: 'esm',
};

// Node.js build
await esbuild.build({
  ...baseConfig,
  outfile: 'dist/index.js',
  external: [
    '@aws-sdk/*',
    '@duckdb/node-api',
    'debug',
    'jsonpath',
    'lru-cache'
  ],
});

// Browser build
await esbuild.build({
  ...baseConfig,
  outfile: 'dist/browser.js',
  platform: 'browser',
  format: 'esm',
  external: ['@duckdb/node-api'], // Only exclude DuckDB as it's Node.js specific
  define: {
    'process.env.DEBUG': 'false'
  },
});
