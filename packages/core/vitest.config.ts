import { defineConfig } from 'vitest/config';
// fileParallelism: false — integration tests across files (schema.test.ts, products/queries.test.ts)
// truncate and reseed shared tables in the same Postgres database; running test files concurrently
// races those truncates/inserts against each other. Sequential file execution keeps them isolated.
export default defineConfig({
  test: { include: ['src/**/*.test.ts'], testTimeout: 30000, hookTimeout: 30000, fileParallelism: false },
});
