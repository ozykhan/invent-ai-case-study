import { build } from 'esbuild';

await build({
  entryPoints: ['src/handlers/lambda.ts'],
  outfile: 'dist/lambda.mjs',
  bundle: true,
  platform: 'node',
  target: 'node22',
  format: 'esm',
  sourcemap: true,
  external: ['@aws-sdk/*', 'pg-native'],
  banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" },
  logLevel: 'info',
});
