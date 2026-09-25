import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const dir = path.resolve('packages/core/drizzle');
const files = readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
const body = files
  .map((f) => `-- ${f}\n` + readFileSync(path.join(dir, f), 'utf8').replaceAll('--> statement-breakpoint', ''))
  .join('\n\n');
writeFileSync('schema.sql', `-- ModaCo Promotion Management API: PostgreSQL DDL\n-- Generated from packages/core/drizzle by scripts/export-schema.ts\n\n${body}`);
console.log(`wrote schema.sql from ${files.length} migration(s)`);
