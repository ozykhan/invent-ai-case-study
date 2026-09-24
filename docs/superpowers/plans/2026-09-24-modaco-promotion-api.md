# ModaCo Promotion Management API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ModaCo products and promotions REST API with a read-time effective price cached in Redis, plus a serverless byte-range fan-out ingestion pipeline for 500k-row vendor files, runnable locally with Docker Compose and LocalStack.

**Architecture:** A pnpm monorepo. `packages/core` holds the Drizzle schema, the effective-price SQL, the pricing rules, cache helpers, and the byte-range chunking logic. `apps/api` is an Express 5 service that reads products through Redis with version-validated cache entries and writes promotions with a single version bump. `apps/ingest` holds a splitter Lambda that enqueues byte ranges without reading the file and a worker Lambda that streams one range, prices rows, and bulk upserts. A local runner spawns each handler in a child process with a hard timeout and memory cap.

**Tech Stack:** Node.js 22, TypeScript 5, Express 5, Drizzle ORM 0.44 with node-postgres, PostgreSQL 16, ioredis 5, Zod 3, AWS SDK v3 (S3, SQS), esbuild, AWS SAM, Vitest 3, supertest, pino, LocalStack 4, Docker Compose, pnpm 9.

Spec: `docs/superpowers/specs/2026-09-24-modaco-promotion-api-design.md`

## Global Constraints

- Runtime: Node.js 22, TypeScript strict mode, ESM (`"type": "module"`), `moduleResolution: "Bundler"` so imports are extensionless.
- Money is a string with two decimals in JSON, `numeric(12,2)` in Postgres, and integer cents inside pricing code. Never a JS float in arithmetic.
- "Active promotion" is always derived: `cancelled_at is null and starts_at <= now and now < ends_at`. Never stored.
- Promotion conflict rule: most recent `created_at` wins, resolved at read time. Overlaps are allowed.
- Product-scoped and category-scoped promotions are both candidates; the lateral subquery orders by `created_at desc, id desc limit 1`.
- Every cache read must survive Redis being down by falling through to Postgres. No read endpoint may fail because of Redis.
- Version bumps happen after the database commit, never inside the transaction.
- Default cache TTL is 300 seconds, capped by the next promotion boundary.
- Ingestion chunk size default 4 MB (`CHUNK_SIZE_BYTES=4194304`), over-read 65536 bytes, max line 65536 bytes, upsert batch size 1000, worker timeout 60 s, worker memory 256 MB, worker reserved concurrency 10, SQS visibility timeout 360 s, max receive count 3.
- Vendor CSV columns, in order: `sku,name,category,vendor_price,stock`. Header row present. UTF-8. No embedded newlines.
- Pricing rules order: validate, margin, round up to `.99`, clamp to category floor and ceiling. Default category margin 30%, floor 0.99, ceiling 99999.99.
- Local infrastructure names: bucket `modaco-vendor-uploads`, queues `modaco-s3-events`, `modaco-ingest-chunks`, `modaco-ingest-dlq`, LocalStack account `000000000000`, region `us-east-1`, credentials `test`/`test`.
- Postgres local URL `postgres://modaco:modaco@localhost:5433/modaco`, Redis local URL `redis://localhost:6379`.
- All commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Integration tests need the compose infrastructure running: `docker compose up -d postgres redis localstack`.

---

## File Structure

```
package.json                         pnpm workspace root, shared scripts
pnpm-workspace.yaml
tsconfig.base.json
.gitignore
.env.example
docker-compose.yml                   postgres, redis, localstack (+ api, runner under profile "app")
Dockerfile                           node:22 image for api and runner services
docker/localstack/init-aws.sh        creates bucket, queues, redrive policy, S3 notification

packages/core/
  package.json, tsconfig.json, vitest.config.ts, drizzle.config.ts
  drizzle/                           generated migrations (committed)
  src/index.ts                       re-exports
  src/money.ts                       cents helpers, round-up-to-.99, clamp
  src/money.test.ts
  src/slug.ts                        slugify
  src/db/schema.ts                   all tables, enums, indexes, checks
  src/db/client.ts                   createDb, Db and Tx types
  src/db/migrate.ts                  runMigrations(db)
  src/db/migrate-cli.ts              CLI entry for pnpm db:migrate
  src/db/schema.test.ts              migrations apply, constraints hold (integration)
  src/pricing/rules.ts               rawVendorRowSchema, priceVendorRow
  src/pricing/rules.test.ts
  src/products/queries.ts            effective price SQL, fetchProductById, fetchProductPage, fetchStock, nextPromotionBoundary
  src/products/queries.test.ts       integration against Postgres
  src/cache/keys.ts                  key builders, ttlSeconds
  src/cache/keys.test.ts
  src/cache/redis.ts                 createRedis
  src/cache/versions.ts              getVersion(s), bumpCategory, bumpProduct
  src/cache/read-through.ts          readThrough with lock coalescing and Redis fallback
  src/cache/read-through.test.ts     integration against Redis
  src/ingest/chunking.ts             computeChunks, rangeFor, ownedLines
  src/ingest/chunking.test.ts
  src/ingest/csv.ts                  parseCsvLine, rowFromFields
  src/ingest/csv.test.ts

apps/api/
  package.json, tsconfig.json, vitest.config.ts
  src/config.ts                      env parsing
  src/logger.ts
  src/errors.ts                      HttpError and helpers
  src/middleware/request-id.ts
  src/middleware/validate.ts         Zod validation into res.locals.input
  src/middleware/error-handler.ts
  src/deps.ts                        AppDeps type, createDeps(config)
  src/app.ts                         createApp(deps)
  src/server.ts                      entrypoint
  src/routes/health.ts
  src/products/schemas.ts            Zod schemas
  src/products/stock.ts              loadStocks (Redis counters with DB fallback)
  src/products/cached-reads.ts       getCachedProduct, getCachedProductPage, resolveCategoryId
  src/products/service.ts            ProductService (reads + create + stock)
  src/products/routes.ts
  src/promotions/schemas.ts
  src/promotions/service.ts          PromotionService
  src/promotions/routes.ts
  src/ingestion/service.ts           IngestionService (presign, job status, rejections)
  src/ingestion/routes.ts
  test/helpers.ts                    test deps, truncate, seed helpers, supertest agent
  test/products.test.ts
  test/promotions.test.ts
  test/ingestion-routes.test.ts

apps/ingest/
  package.json, tsconfig.json, vitest.config.ts, build.mjs
  src/config.ts                      env parsing shared by handlers and runner
  src/aws.ts                         S3 and SQS client factories
  src/deps.ts                        lazily built singleton deps for handlers
  src/splitter.ts                    splitUpload
  src/worker.ts                      processChunk
  src/dlq.ts                         handleDeadLetter
  src/handlers/lambda.ts             S3Handler and SQSHandler adapters (esbuild entry)
  src/invoke.ts                      child-process entry: handler name + event on stdin
  src/queue.ts                       pollOnce(sqs, queueUrl, invoke)
  src/runner.ts                      local runner loops, spawns invoke.ts with timeout and memory cap
  test/helpers.ts
  test/splitter.test.ts
  test/worker.test.ts
  test/end-to-end.test.ts            upload via presigned URL, drive queues, verify rows

infra/template.yaml                  SAM template
scripts/export-schema.ts             concatenates migrations into schema.sql
scripts/generate-vendor-file.ts
scripts/seed.ts
scripts/demo-ingest.ts
scripts/demo-flash-sale.ts
schema.sql, README.md, ADR.md, AI_APPENDIX.md
```

---

### Task 1: Monorepo scaffold and local infrastructure

**Files:**
- Create: `package.json`, `pnpm-workspace.yaml`, `tsconfig.base.json`, `.gitignore`, `.env.example`, `docker-compose.yml`, `Dockerfile`, `docker/localstack/init-aws.sh`
- Create: `packages/core/package.json`, `packages/core/tsconfig.json`, `packages/core/vitest.config.ts`, `packages/core/src/index.ts`, `packages/core/src/money.ts`, `packages/core/src/money.test.ts`, `packages/core/src/slug.ts`

**Interfaces:**
- Produces: `toCents(value: string | number): number`, `fromCents(cents: number): string`, `roundUpTo99(cents: number): number`, `clampCents(cents: number, floor: number, ceiling: number): number`, `slugify(name: string): string`. All exported from `@modaco/core`.

- [ ] **Step 1: Root files**

`package.json`:
```json
{
  "name": "modaco-promotions",
  "private": true,
  "type": "module",
  "packageManager": "pnpm@9.12.0",
  "engines": { "node": ">=22" },
  "scripts": {
    "typecheck": "pnpm -r typecheck",
    "test": "pnpm -r test",
    "test:unit": "pnpm --filter @modaco/core test",
    "db:generate": "pnpm --filter @modaco/core db:generate",
    "db:migrate": "pnpm --filter @modaco/core db:migrate",
    "schema:export": "tsx scripts/export-schema.ts",
    "dev:api": "pnpm --filter @modaco/api dev",
    "dev:runner": "pnpm --filter @modaco/ingest runner",
    "build:lambda": "pnpm --filter @modaco/ingest build",
    "seed": "tsx scripts/seed.ts",
    "vendor-file": "tsx scripts/generate-vendor-file.ts",
    "demo:ingest": "tsx scripts/demo-ingest.ts",
    "demo:flash-sale": "tsx scripts/demo-flash-sale.ts"
  },
  "dependencies": {
    "@modaco/core": "workspace:*"
  },
  "devDependencies": {
    "@types/node": "^22.7.5",
    "tsx": "^4.19.1",
    "typescript": "^5.6.3",
    "vitest": "^3.0.5"
  }
}
```

`pnpm-workspace.yaml`:
```yaml
packages:
  - "packages/*"
  - "apps/*"
```

`tsconfig.base.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "types": ["node"]
  }
}
```

`.gitignore`:
```
node_modules
dist
.env
*.log
tmp/
.localstack/
```

`.env.example`:
```
PORT=3000
DATABASE_URL=postgres://modaco:modaco@localhost:5433/modaco
REDIS_URL=redis://localhost:6379
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://localhost:4566
S3_PUBLIC_ENDPOINT=http://localhost:4566
S3_BUCKET=modaco-vendor-uploads
S3_EVENTS_QUEUE_URL=http://localhost:4566/000000000000/modaco-s3-events
CHUNK_QUEUE_URL=http://localhost:4566/000000000000/modaco-ingest-chunks
DLQ_URL=http://localhost:4566/000000000000/modaco-ingest-dlq
CHUNK_SIZE_BYTES=4194304
UPSERT_BATCH_SIZE=1000
LAMBDA_TIMEOUT_MS=60000
LAMBDA_MEMORY_MB=256
LOG_LEVEL=info
```

- [ ] **Step 2: Docker Compose, Dockerfile, LocalStack init**

`docker-compose.yml`:
```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: modaco
      POSTGRES_PASSWORD: modaco
      POSTGRES_DB: modaco
    ports: ["5433:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U modaco"]
      interval: 3s
      timeout: 3s
      retries: 20

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s
      timeout: 3s
      retries: 20

  localstack:
    image: localstack/localstack:4
    environment:
      SERVICES: s3,sqs
      DEBUG: "0"
    ports: ["4566:4566"]
    volumes:
      - ./docker/localstack:/etc/localstack/init/ready.d:ro
    healthcheck:
      test: ["CMD-SHELL", "awslocal s3api head-bucket --bucket modaco-vendor-uploads"]
      interval: 5s
      timeout: 5s
      retries: 30

  api:
    profiles: ["app"]
    build: .
    command: pnpm --filter @modaco/api start
    environment:
      PORT: 3000
      DATABASE_URL: postgres://modaco:modaco@postgres:5432/modaco
      REDIS_URL: redis://redis:6379
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_ENDPOINT_URL: http://localstack:4566
      S3_PUBLIC_ENDPOINT: http://localhost:4566
      S3_BUCKET: modaco-vendor-uploads
    ports: ["3000:3000"]
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      localstack: { condition: service_healthy }

  runner:
    profiles: ["app"]
    build: .
    command: pnpm --filter @modaco/ingest runner
    environment:
      DATABASE_URL: postgres://modaco:modaco@postgres:5432/modaco
      REDIS_URL: redis://redis:6379
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_ENDPOINT_URL: http://localstack:4566
      S3_BUCKET: modaco-vendor-uploads
      S3_EVENTS_QUEUE_URL: http://localstack:4566/000000000000/modaco-s3-events
      CHUNK_QUEUE_URL: http://localstack:4566/000000000000/modaco-ingest-chunks
      DLQ_URL: http://localstack:4566/000000000000/modaco-ingest-dlq
      CHUNK_SIZE_BYTES: 4194304
      UPSERT_BATCH_SIZE: 1000
      LAMBDA_TIMEOUT_MS: 60000
      LAMBDA_MEMORY_MB: 256
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      localstack: { condition: service_healthy }
```

`Dockerfile`:
```dockerfile
FROM node:22-alpine
RUN corepack enable
WORKDIR /app
COPY . .
RUN pnpm install --frozen-lockfile
```

`docker/localstack/init-aws.sh` (make executable with `chmod +x`):
```bash
#!/bin/bash
set -euo pipefail

awslocal s3 mb s3://modaco-vendor-uploads || true

awslocal sqs create-queue --queue-name modaco-ingest-dlq >/dev/null
DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/modaco-ingest-dlq \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awslocal sqs create-queue --queue-name modaco-ingest-chunks \
  --attributes "{\"VisibilityTimeout\":\"360\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" >/dev/null

awslocal sqs create-queue --queue-name modaco-s3-events \
  --attributes '{"VisibilityTimeout":"60"}' >/dev/null

awslocal s3api put-bucket-notification-configuration \
  --bucket modaco-vendor-uploads \
  --notification-configuration '{
    "QueueConfigurations": [{
      "Id": "uploads",
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:modaco-s3-events",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "uploads/"}]}}
    }]
  }'

echo "localstack resources ready"
```

- [ ] **Step 3: Core package skeleton**

`packages/core/package.json`:
```json
{
  "name": "@modaco/core",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "main": "./src/index.ts",
  "types": "./src/index.ts",
  "scripts": {
    "test": "vitest run",
    "typecheck": "tsc --noEmit",
    "db:generate": "drizzle-kit generate",
    "db:migrate": "tsx src/db/migrate-cli.ts"
  },
  "dependencies": {
    "drizzle-orm": "^0.44.2",
    "ioredis": "^5.4.1",
    "pg": "^8.13.1",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@types/pg": "^8.11.10",
    "drizzle-kit": "^0.31.1",
    "tsx": "^4.19.1",
    "typescript": "^5.6.3",
    "vitest": "^3.0.5"
  }
}
```

`packages/core/tsconfig.json`:
```json
{ "extends": "../../tsconfig.base.json", "include": ["src", "drizzle.config.ts"] }
```

`packages/core/vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';
export default defineConfig({ test: { include: ['src/**/*.test.ts'], testTimeout: 30000, hookTimeout: 30000 } });
```

`packages/core/src/index.ts` (grows in later tasks):
```ts
export * from './money';
export * from './slug';
```

- [ ] **Step 4: Write the failing money tests**

`packages/core/src/money.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { clampCents, fromCents, roundUpTo99, toCents } from './money';

describe('toCents', () => {
  it('parses decimal strings exactly', () => {
    expect(toCents('12.34')).toBe(1234);
    expect(toCents('12')).toBe(1200);
    expect(toCents('0.5')).toBe(50);
    expect(toCents(19.99)).toBe(1999);
  });
  it('rejects invalid input', () => {
    expect(() => toCents('abc')).toThrow();
    expect(() => toCents('1.234')).toThrow();
    expect(() => toCents('-1')).toThrow();
  });
});

describe('fromCents', () => {
  it('formats with two decimals', () => {
    expect(fromCents(1234)).toBe('12.34');
    expect(fromCents(5)).toBe('0.05');
    expect(fromCents(0)).toBe('0.00');
  });
});

describe('roundUpTo99', () => {
  it('rounds up to the next .99 price point', () => {
    expect(roundUpTo99(1234)).toBe(1299);
    expect(roundUpTo99(1299)).toBe(1299);
    expect(roundUpTo99(1300)).toBe(1399);
    expect(roundUpTo99(1)).toBe(99);
    expect(roundUpTo99(0)).toBe(99);
  });
});

describe('clampCents', () => {
  it('clamps into the inclusive range', () => {
    expect(clampCents(50, 99, 1000)).toBe(99);
    expect(clampCents(5000, 99, 1000)).toBe(1000);
    expect(clampCents(500, 99, 1000)).toBe(500);
  });
});
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `pnpm install && pnpm --filter @modaco/core test`
Expected: FAIL, cannot resolve `./money`.

- [ ] **Step 6: Implement money and slug**

`packages/core/src/money.ts`:
```ts
const DECIMAL_RE = /^\d+(\.\d{1,2})?$/;

/** Parse a non-negative decimal (string or number) into integer cents. Throws on invalid input. */
export function toCents(value: string | number): number {
  const s = typeof value === 'number' ? value.toFixed(2) : value.trim();
  if (!DECIMAL_RE.test(s)) throw new Error(`invalid money value: ${String(value)}`);
  const [whole, frac = ''] = s.split('.');
  return Number(whole) * 100 + Number(frac.padEnd(2, '0'));
}

export function fromCents(cents: number): string {
  if (!Number.isInteger(cents) || cents < 0) throw new Error(`invalid cents: ${cents}`);
  const whole = Math.floor(cents / 100);
  const frac = cents % 100;
  return `${whole}.${frac.toString().padStart(2, '0')}`;
}

/** Smallest x.99 price point that is >= cents. */
export function roundUpTo99(cents: number): number {
  const candidate = Math.floor(cents / 100) * 100 + 99;
  return candidate >= cents ? candidate : candidate + 100;
}

export function clampCents(cents: number, floor: number, ceiling: number): number {
  return Math.min(Math.max(cents, floor), ceiling);
}
```

`packages/core/src/slug.ts`:
```ts
export function slugify(name: string): string {
  return name
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'category';
}
```

- [ ] **Step 7: Run tests and start infrastructure**

Run: `pnpm --filter @modaco/core test`
Expected: PASS, 8 tests.

Run: `chmod +x docker/localstack/init-aws.sh && docker compose up -d postgres redis localstack && sleep 15 && docker compose ps`
Expected: all three services `healthy`.

Run: `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url http://localhost:4566 sqs list-queues`
Expected: three queue URLs listed. (If `aws` CLI is not installed, run `docker compose exec localstack awslocal sqs list-queues`.)

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "chore: scaffold monorepo, compose infrastructure, core money helpers

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Database schema, migrations, and client

**Files:**
- Create: `packages/core/drizzle.config.ts`, `packages/core/src/db/schema.ts`, `packages/core/src/db/client.ts`, `packages/core/src/db/migrate.ts`, `packages/core/src/db/migrate-cli.ts`, `packages/core/src/db/schema.test.ts`, `packages/core/drizzle/*` (generated), `scripts/export-schema.ts`, `schema.sql`
- Modify: `packages/core/src/index.ts`

**Interfaces:**
- Produces: `createDb(url, { max? }) => { db: Db; pool: Pool; close(): Promise<void> }`, types `Db`, `Tx`, `DbOrTx`, `runMigrations(db)`, and the table objects `categories`, `products`, `promotions`, `ingestionJobs`, `ingestionChunks`, `ingestionRejections` plus enums `discountTypeEnum`, `promotionScopeEnum`, `jobStatusEnum`, `chunkStatusEnum`. Also `TEST_DATABASE_URL` default constant.

- [ ] **Step 1: Write the schema**

`packages/core/src/db/schema.ts`:
```ts
import { sql } from 'drizzle-orm';
import {
  bigint, check, index, integer, numeric, pgEnum, pgTable, text, timestamp, uniqueIndex, uuid,
} from 'drizzle-orm/pg-core';

export const discountTypeEnum = pgEnum('discount_type', ['percentage', 'fixed']);
export const promotionScopeEnum = pgEnum('promotion_scope', ['product', 'category']);
export const jobStatusEnum = pgEnum('ingestion_job_status', ['pending', 'splitting', 'processing', 'completed', 'failed']);
export const chunkStatusEnum = pgEnum('ingestion_chunk_status', ['pending', 'processing', 'completed', 'failed']);

const ts = (name: string) => timestamp(name, { withTimezone: true });

export const categories = pgTable('categories', {
  id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
  name: text('name').notNull().unique(),
  slug: text('slug').notNull().unique(),
  marginPct: numeric('margin_pct', { precision: 6, scale: 2 }).notNull().default('30.00'),
  priceFloor: numeric('price_floor', { precision: 12, scale: 2 }).notNull().default('0.99'),
  priceCeiling: numeric('price_ceiling', { precision: 12, scale: 2 }).notNull().default('99999.99'),
});

export const products = pgTable('products', {
  id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
  sku: text('sku').notNull().unique(),
  name: text('name').notNull(),
  categoryId: integer('category_id').notNull().references(() => categories.id),
  basePrice: numeric('base_price', { precision: 12, scale: 2 }).notNull(),
  stock: integer('stock').notNull().default(0),
  createdAt: ts('created_at').notNull().defaultNow(),
  updatedAt: ts('updated_at').notNull().defaultNow(),
}, (t) => [
  index('products_category_base_price_idx').on(t.categoryId, t.basePrice),
  check('products_stock_nonnegative', sql`${t.stock} >= 0`),
  check('products_base_price_nonnegative', sql`${t.basePrice} >= 0`),
]);

export const promotions = pgTable('promotions', {
  id: uuid('id').primaryKey().defaultRandom(),
  name: text('name').notNull(),
  discountType: discountTypeEnum('discount_type').notNull(),
  value: numeric('value', { precision: 12, scale: 2 }).notNull(),
  startsAt: ts('starts_at').notNull(),
  endsAt: ts('ends_at').notNull(),
  scope: promotionScopeEnum('scope').notNull(),
  productId: integer('product_id').references(() => products.id),
  categoryId: integer('category_id').references(() => categories.id),
  cancelledAt: ts('cancelled_at'),
  createdAt: ts('created_at').notNull().defaultNow(),
}, (t) => [
  index('promotions_category_window_idx').on(t.categoryId, t.startsAt, t.endsAt).where(sql`${t.cancelledAt} is null`),
  index('promotions_product_window_idx').on(t.productId, t.startsAt, t.endsAt).where(sql`${t.cancelledAt} is null`),
  check('promotions_scope_target', sql`(${t.scope} = 'product' and ${t.productId} is not null and ${t.categoryId} is null) or (${t.scope} = 'category' and ${t.categoryId} is not null and ${t.productId} is null)`),
  check('promotions_window', sql`${t.endsAt} > ${t.startsAt}`),
  check('promotions_value', sql`${t.value} >= 0 and (${t.discountType} <> 'percentage' or ${t.value} <= 100)`),
]);

export const ingestionJobs = pgTable('ingestion_jobs', {
  id: uuid('id').primaryKey().defaultRandom(),
  s3Key: text('s3_key').notNull(),
  status: jobStatusEnum('status').notNull().default('pending'),
  totalChunks: integer('total_chunks').notNull().default(0),
  completedChunks: integer('completed_chunks').notNull().default(0),
  failedChunks: integer('failed_chunks').notNull().default(0),
  rowsProcessed: integer('rows_processed').notNull().default(0),
  rowsRejected: integer('rows_rejected').notNull().default(0),
  error: text('error'),
  createdAt: ts('created_at').notNull().defaultNow(),
  updatedAt: ts('updated_at').notNull().defaultNow(),
});

export const ingestionChunks = pgTable('ingestion_chunks', {
  id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
  jobId: uuid('job_id').notNull().references(() => ingestionJobs.id),
  chunkIndex: integer('chunk_index').notNull(),
  byteStart: bigint('byte_start', { mode: 'number' }).notNull(),
  byteEnd: bigint('byte_end', { mode: 'number' }).notNull(),
  status: chunkStatusEnum('status').notNull().default('pending'),
  attempts: integer('attempts').notNull().default(0),
  rowsProcessed: integer('rows_processed').notNull().default(0),
  rowsRejected: integer('rows_rejected').notNull().default(0),
  error: text('error'),
  updatedAt: ts('updated_at').notNull().defaultNow(),
}, (t) => [
  uniqueIndex('ingestion_chunks_job_index_uq').on(t.jobId, t.chunkIndex),
]);

export const ingestionRejections = pgTable('ingestion_rejections', {
  id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
  jobId: uuid('job_id').notNull().references(() => ingestionJobs.id),
  chunkIndex: integer('chunk_index').notNull(),
  lineNumber: integer('line_number').notNull(),
  rawLine: text('raw_line').notNull(),
  reason: text('reason').notNull(),
}, (t) => [
  index('ingestion_rejections_job_idx').on(t.jobId),
]);
```

`packages/core/drizzle.config.ts`:
```ts
import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'postgresql',
  schema: './src/db/schema.ts',
  out: './drizzle',
  dbCredentials: { url: process.env.DATABASE_URL ?? 'postgres://modaco:modaco@localhost:5433/modaco' },
});
```

- [ ] **Step 2: Client and migrator**

`packages/core/src/db/client.ts`:
```ts
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import * as schema from './schema';

export type Db = NodePgDatabase<typeof schema>;
export type Tx = Parameters<Parameters<Db['transaction']>[0]>[0];
export type DbOrTx = Db | Tx;

export const TEST_DATABASE_URL = 'postgres://modaco:modaco@localhost:5433/modaco';

export function createDb(connectionString: string, opts: { max?: number } = {}) {
  const pool = new pg.Pool({ connectionString, max: opts.max ?? 10 });
  const db = drizzle(pool, { schema });
  return { db, pool, close: () => pool.end() };
}
```

`packages/core/src/db/migrate.ts`:
```ts
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import type { Db } from './client';

export const MIGRATIONS_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'drizzle');

export async function runMigrations(db: Db, migrationsFolder = MIGRATIONS_DIR): Promise<void> {
  await migrate(db, { migrationsFolder });
}
```

`packages/core/src/db/migrate-cli.ts`:
```ts
import { createDb, TEST_DATABASE_URL } from './client';
import { runMigrations } from './migrate';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 1 });
await runMigrations(db);
await close();
console.log('migrations applied');
```

Add to `packages/core/src/index.ts`:
```ts
export * from './db/schema';
export * from './db/client';
export * from './db/migrate';
```

- [ ] **Step 3: Generate the migration**

Run: `pnpm install && pnpm db:generate`
Expected: `packages/core/drizzle/0000_<name>.sql` and `packages/core/drizzle/meta/` created. Open the SQL and confirm it contains the four `CREATE TYPE` statements, six `CREATE TABLE` statements, the three `CONSTRAINT ... CHECK` lines on promotions, and the two partial indexes with `WHERE "cancelled_at" is null`.

- [ ] **Step 4: Write the failing schema test**

`packages/core/src/db/schema.test.ts`:
```ts
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDb, TEST_DATABASE_URL } from './client';
import { runMigrations } from './migrate';
import { categories, products, promotions } from './schema';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });

beforeAll(async () => {
  await runMigrations(db);
  await db.execute(sql`truncate promotions, products, categories restart identity cascade`);
});
afterAll(() => close());

describe('schema', () => {
  it('creates all tables', async () => {
    const res = await db.execute(sql`select table_name from information_schema.tables where table_schema = 'public' order by table_name`);
    const names = res.rows.map((r) => r.table_name);
    expect(names).toEqual(expect.arrayContaining([
      'categories', 'products', 'promotions', 'ingestion_jobs', 'ingestion_chunks', 'ingestion_rejections',
    ]));
  });

  it('rejects a promotion whose scope does not match its target', async () => {
    const [cat] = await db.insert(categories).values({ name: 'Accessories', slug: 'accessories' }).returning();
    await expect(db.insert(promotions).values({
      name: 'bad', discountType: 'percentage', value: '10', scope: 'product',
      categoryId: cat!.id, startsAt: new Date('2026-01-01'), endsAt: new Date('2026-02-01'),
    })).rejects.toThrow(/promotions_scope_target/);
  });

  it('rejects a percentage over 100 and an inverted window', async () => {
    const [cat] = await db.select().from(categories).limit(1);
    await expect(db.insert(promotions).values({
      name: 'bad', discountType: 'percentage', value: '150', scope: 'category',
      categoryId: cat!.id, startsAt: new Date('2026-01-01'), endsAt: new Date('2026-02-01'),
    })).rejects.toThrow(/promotions_value/);
    await expect(db.insert(promotions).values({
      name: 'bad', discountType: 'fixed', value: '5', scope: 'category',
      categoryId: cat!.id, startsAt: new Date('2026-02-01'), endsAt: new Date('2026-01-01'),
    })).rejects.toThrow(/promotions_window/);
  });

  it('rejects negative stock and duplicate sku', async () => {
    const [cat] = await db.select().from(categories).limit(1);
    await expect(db.insert(products).values({ sku: 'A', name: 'a', categoryId: cat!.id, basePrice: '1.00', stock: -1 }))
      .rejects.toThrow(/products_stock_nonnegative/);
    await db.insert(products).values({ sku: 'A', name: 'a', categoryId: cat!.id, basePrice: '1.00', stock: 1 });
    await expect(db.insert(products).values({ sku: 'A', name: 'b', categoryId: cat!.id, basePrice: '1.00', stock: 1 }))
      .rejects.toThrow(/duplicate key/);
  });
});
```

- [ ] **Step 5: Run the schema test**

Run: `pnpm --filter @modaco/core test src/db/schema.test.ts`
Expected: PASS, 4 tests (the infrastructure from Task 1 must be up).

- [ ] **Step 6: Schema export script**

`scripts/export-schema.ts`:
```ts
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const dir = path.resolve('packages/core/drizzle');
const files = readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
const body = files
  .map((f) => `-- ${f}\n` + readFileSync(path.join(dir, f), 'utf8').replaceAll('--> statement-breakpoint', ''))
  .join('\n\n');
writeFileSync('schema.sql', `-- ModaCo Promotion Management API: PostgreSQL DDL\n-- Generated from packages/core/drizzle by scripts/export-schema.ts\n\n${body}`);
console.log(`wrote schema.sql from ${files.length} migration(s)`);
```

Run: `pnpm schema:export && head -30 schema.sql`
Expected: `schema.sql` at the repo root starting with the header and `CREATE TYPE "public"."discount_type"`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(core): drizzle schema, migrations, db client, schema.sql export

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Pricing rules and CSV parsing

**Files:**
- Create: `packages/core/src/pricing/rules.ts`, `packages/core/src/pricing/rules.test.ts`, `packages/core/src/ingest/csv.ts`, `packages/core/src/ingest/csv.test.ts`
- Modify: `packages/core/src/index.ts`

**Interfaces:**
- Produces:
  - `VENDOR_COLUMNS: readonly ['sku','name','category','vendor_price','stock']`
  - `parseCsvLine(line: string): string[]`
  - `rowFromFields(fields: string[]): Record<string, string> | null` (null when the field count is wrong)
  - `interface CategoryPricing { marginPct: number; priceFloorCents: number; priceCeilingCents: number }`
  - `DEFAULT_CATEGORY_PRICING: CategoryPricing` = `{ marginPct: 30, priceFloorCents: 99, priceCeilingCents: 9999999 }`
  - `interface PricedRow { sku: string; name: string; category: string; basePriceCents: number; stock: number }`
  - `type PricingOutcome = { ok: true; row: PricedRow } | { ok: false; reason: string }`
  - `priceVendorRow(input: Record<string, string>, pricingFor: (category: string) => CategoryPricing): PricingOutcome`
  - `categoryPricingFromRow(row: { marginPct: string; priceFloor: string; priceCeiling: string }): CategoryPricing`

- [ ] **Step 1: Write the failing pricing tests**

`packages/core/src/pricing/rules.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { DEFAULT_CATEGORY_PRICING, priceVendorRow, type CategoryPricing } from './rules';

const defaults = () => DEFAULT_CATEGORY_PRICING;
const row = (over: Partial<Record<string, string>> = {}) => ({
  sku: 'SKU-1', name: 'Belt', category: 'Accessories', vendor_price: '10.00', stock: '5', ...over,
});

describe('priceVendorRow', () => {
  it('applies margin, rounds up to .99, and keeps stock', () => {
    const out = priceVendorRow(row(), defaults);
    expect(out).toEqual({ ok: true, row: { sku: 'SKU-1', name: 'Belt', category: 'Accessories', basePriceCents: 1399, stock: 5 } });
  });

  it('uses the category margin', () => {
    const pricing: CategoryPricing = { marginPct: 100, priceFloorCents: 99, priceCeilingCents: 9999999 };
    const out = priceVendorRow(row(), () => pricing);
    expect(out.ok && out.row.basePriceCents).toBe(2099);
  });

  it('clamps to the category floor and ceiling after rounding', () => {
    const floor: CategoryPricing = { marginPct: 0, priceFloorCents: 1999, priceCeilingCents: 9999999 };
    expect(priceVendorRow(row({ vendor_price: '1.00' }), () => floor)).toMatchObject({ ok: true, row: { basePriceCents: 1999 } });
    const ceiling: CategoryPricing = { marginPct: 0, priceFloorCents: 99, priceCeilingCents: 500 };
    expect(priceVendorRow(row({ vendor_price: '100.00' }), () => ceiling)).toMatchObject({ ok: true, row: { basePriceCents: 500 } });
  });

  it('rejects invalid rows with a reason', () => {
    expect(priceVendorRow(row({ sku: '' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('sku') });
    expect(priceVendorRow(row({ vendor_price: '0' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ vendor_price: '-3' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ vendor_price: 'abc' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ stock: '1.5' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ stock: '-1' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ category: '   ' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('category') });
  });

  it('trims whitespace on text fields', () => {
    const out = priceVendorRow(row({ sku: '  SKU-9 ', name: ' Hat ' }), defaults);
    expect(out.ok && out.row).toMatchObject({ sku: 'SKU-9', name: 'Hat' });
  });
});
```

`packages/core/src/ingest/csv.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { parseCsvLine, rowFromFields, VENDOR_COLUMNS } from './csv';

describe('parseCsvLine', () => {
  it('splits plain fields', () => {
    expect(parseCsvLine('a,b,c')).toEqual(['a', 'b', 'c']);
  });
  it('handles quoted fields with commas and escaped quotes', () => {
    expect(parseCsvLine('SKU-1,"Belt, leather",Accessories,"10.00",5')).toEqual(['SKU-1', 'Belt, leather', 'Accessories', '10.00', '5']);
    expect(parseCsvLine('a,"say ""hi""",c')).toEqual(['a', 'say "hi"', 'c']);
  });
  it('keeps empty fields', () => {
    expect(parseCsvLine('a,,c')).toEqual(['a', '', 'c']);
  });
});

describe('rowFromFields', () => {
  it('maps fields to the vendor columns', () => {
    expect(rowFromFields(['s', 'n', 'c', '1.00', '2'])).toEqual({ sku: 's', name: 'n', category: 'c', vendor_price: '1.00', stock: '2' });
    expect(VENDOR_COLUMNS).toHaveLength(5);
  });
  it('returns null on a wrong field count', () => {
    expect(rowFromFields(['a', 'b'])).toBeNull();
    expect(rowFromFields(['a', 'b', 'c', 'd', 'e', 'f'])).toBeNull();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm --filter @modaco/core test src/pricing src/ingest/csv.test.ts`
Expected: FAIL, modules not found.

- [ ] **Step 3: Implement pricing rules**

`packages/core/src/pricing/rules.ts`:
```ts
import { z } from 'zod';
import { clampCents, roundUpTo99, toCents } from '../money';

export interface CategoryPricing {
  marginPct: number;
  priceFloorCents: number;
  priceCeilingCents: number;
}

export const DEFAULT_CATEGORY_PRICING: CategoryPricing = { marginPct: 30, priceFloorCents: 99, priceCeilingCents: 9999999 };

export function categoryPricingFromRow(row: { marginPct: string; priceFloor: string; priceCeiling: string }): CategoryPricing {
  return {
    marginPct: Number(row.marginPct),
    priceFloorCents: toCents(row.priceFloor),
    priceCeilingCents: toCents(row.priceCeiling),
  };
}

export const rawVendorRowSchema = z.object({
  sku: z.string().trim().min(1, 'sku is required').max(64),
  name: z.string().trim().min(1, 'name is required').max(255),
  category: z.string().trim().min(1, 'category is required').max(100),
  vendor_price: z.string().trim().regex(/^\d+(\.\d{1,2})?$/, 'vendor_price must be a decimal with up to 2 places'),
  stock: z.string().trim().regex(/^\d+$/, 'stock must be a non-negative integer'),
});

export interface PricedRow {
  sku: string;
  name: string;
  category: string;
  basePriceCents: number;
  stock: number;
}

export type PricingOutcome = { ok: true; row: PricedRow } | { ok: false; reason: string };

/**
 * The application-layer pricing pipeline every vendor row passes through:
 * 1. validate  2. margin  3. round up to .99  4. clamp to category floor/ceiling
 */
export function priceVendorRow(
  input: Record<string, string>,
  pricingFor: (category: string) => CategoryPricing,
): PricingOutcome {
  const parsed = rawVendorRowSchema.safeParse(input);
  if (!parsed.success) {
    const issue = parsed.error.issues[0]!;
    return { ok: false, reason: `validation: ${issue.path.join('.')}: ${issue.message}` };
  }
  const raw = parsed.data;
  const vendorCents = toCents(raw.vendor_price);
  if (vendorCents <= 0) return { ok: false, reason: 'validation: vendor_price must be greater than 0' };

  const pricing = pricingFor(raw.category);
  const withMargin = Math.round(vendorCents * (1 + pricing.marginPct / 100));
  const pricePoint = roundUpTo99(withMargin);
  const basePriceCents = clampCents(pricePoint, pricing.priceFloorCents, pricing.priceCeilingCents);

  return {
    ok: true,
    row: { sku: raw.sku, name: raw.name, category: raw.category, basePriceCents, stock: Number(raw.stock) },
  };
}
```

`packages/core/src/ingest/csv.ts`:
```ts
export const VENDOR_COLUMNS = ['sku', 'name', 'category', 'vendor_price', 'stock'] as const;

/** Minimal RFC 4180 line parser: commas, double quotes, doubled quotes. No embedded newlines. */
export function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { field += '"'; i++; } else { inQuotes = false; }
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      out.push(field); field = '';
    } else {
      field += ch;
    }
  }
  out.push(field);
  return out;
}

export function rowFromFields(fields: string[]): Record<string, string> | null {
  if (fields.length !== VENDOR_COLUMNS.length) return null;
  const row: Record<string, string> = {};
  VENDOR_COLUMNS.forEach((col, i) => { row[col] = fields[i]!; });
  return row;
}
```

Add to `packages/core/src/index.ts`:
```ts
export * from './pricing/rules';
export * from './ingest/csv';
```

- [ ] **Step 4: Run tests**

Run: `pnpm --filter @modaco/core test`
Expected: PASS, all tests including the 5 pricing and 5 csv tests.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): vendor pricing rules and csv line parsing

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Effective price queries

**Files:**
- Create: `packages/core/src/products/queries.ts`, `packages/core/src/products/queries.test.ts`
- Modify: `packages/core/src/index.ts`

**Interfaces:**
- Produces:
  - `interface ProductRecord { id: number; sku: string; name: string; category: { id: number; name: string; slug: string }; basePrice: string; effectivePrice: string; activePromotion: { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string } | null }` (no stock)
  - `type SortDir = 'asc' | 'desc'`
  - `fetchProductById(db: DbOrTx, id: number, now: Date): Promise<ProductRecord | null>`
  - `fetchProductPage(db: DbOrTx, opts: { categoryId: number | null; sort: SortDir; limit: number; offset: number; now: Date }): Promise<{ items: ProductRecord[]; total: number }>`
  - `fetchStock(db: DbOrTx, ids: number[]): Promise<Map<number, number>>`
  - `nextPromotionBoundary(db: DbOrTx, scope: { productId: number; categoryId: number } | { categoryId: number | null }, now: Date): Promise<Date | null>`

- [ ] **Step 1: Write the failing query tests**

`packages/core/src/products/queries.test.ts`:
```ts
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDb, TEST_DATABASE_URL } from '../db/client';
import { runMigrations } from '../db/migrate';
import { categories, products, promotions } from '../db/schema';
import { fetchProductById, fetchProductPage, fetchStock, nextPromotionBoundary } from './queries';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });
const now = new Date('2026-06-15T12:00:00Z');
const day = (d: number) => new Date(now.getTime() + d * 86_400_000);

let accessoriesId: number;
let shoesId: number;
let beltId: number;
let hatId: number;
let bootId: number;

beforeAll(() => runMigrations(db));
afterAll(() => close());

beforeEach(async () => {
  await db.execute(sql`truncate promotions, products, categories restart identity cascade`);
  const [acc, shoes] = await db.insert(categories).values([
    { name: 'Accessories', slug: 'accessories' }, { name: 'Shoes', slug: 'shoes' },
  ]).returning();
  accessoriesId = acc!.id; shoesId = shoes!.id;
  const rows = await db.insert(products).values([
    { sku: 'BELT', name: 'Belt', categoryId: accessoriesId, basePrice: '20.00', stock: 3 },
    { sku: 'HAT', name: 'Hat', categoryId: accessoriesId, basePrice: '10.00', stock: 0 },
    { sku: 'BOOT', name: 'Boot', categoryId: shoesId, basePrice: '100.00', stock: 7 },
  ]).returning();
  beltId = rows[0]!.id; hatId = rows[1]!.id; bootId = rows[2]!.id;
});

const promo = (over: Partial<typeof promotions.$inferInsert>) => db.insert(promotions).values({
  name: 'p', discountType: 'percentage', value: '50', scope: 'category', categoryId: accessoriesId,
  startsAt: day(-1), endsAt: day(1), ...over,
}).returning();

describe('fetchProductById', () => {
  it('returns base price when no promotion is active', async () => {
    const p = await fetchProductById(db, beltId, now);
    expect(p).toMatchObject({ sku: 'BELT', basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, category: { slug: 'accessories' } });
    expect(p).not.toHaveProperty('stock');
  });

  it('applies a category percentage promotion', async () => {
    await promo({});
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('10.00');
    expect((await fetchProductById(db, bootId, now))!.effectivePrice).toBe('100.00');
  });

  it('applies a fixed promotion floored at zero', async () => {
    await promo({ discountType: 'fixed', value: '15.00' });
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('5.00');
    expect((await fetchProductById(db, hatId, now))!.effectivePrice).toBe('0.00');
  });

  it('ignores cancelled and out-of-window promotions', async () => {
    await promo({ cancelledAt: now });
    await promo({ startsAt: day(1), endsAt: day(2) });
    await promo({ startsAt: day(-3), endsAt: day(-2) });
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('20.00');
  });

  it('most recently created promotion wins across scopes', async () => {
    await promo({ scope: 'product', categoryId: null, productId: beltId, value: '10', createdAt: day(-2) });
    const [cat] = await promo({ value: '50', createdAt: day(-1) });
    const p = await fetchProductById(db, beltId, now);
    expect(p!.effectivePrice).toBe('10.00');
    expect(p!.activePromotion!.id).toBe(cat!.id);
  });

  it('returns null for an unknown id', async () => {
    expect(await fetchProductById(db, 999999, now)).toBeNull();
  });
});

describe('fetchProductPage', () => {
  it('filters by category and sorts by effective price', async () => {
    await promo({ scope: 'product', categoryId: null, productId: beltId, value: '90' }); // belt -> 2.00
    const asc = await fetchProductPage(db, { categoryId: accessoriesId, sort: 'asc', limit: 10, offset: 0, now });
    expect(asc.total).toBe(2);
    expect(asc.items.map((i) => i.sku)).toEqual(['BELT', 'HAT']);
    const desc = await fetchProductPage(db, { categoryId: accessoriesId, sort: 'desc', limit: 10, offset: 0, now });
    expect(desc.items.map((i) => i.sku)).toEqual(['HAT', 'BELT']);
  });

  it('paginates across all categories', async () => {
    const page2 = await fetchProductPage(db, { categoryId: null, sort: 'asc', limit: 2, offset: 2, now });
    expect(page2.total).toBe(3);
    expect(page2.items.map((i) => i.sku)).toEqual(['BOOT']);
  });
});

describe('fetchStock', () => {
  it('returns stock for the given ids', async () => {
    const m = await fetchStock(db, [beltId, bootId, 424242]);
    expect(m.get(beltId)).toBe(3);
    expect(m.get(bootId)).toBe(7);
    expect(m.has(424242)).toBe(false);
    expect((await fetchStock(db, [])).size).toBe(0);
  });
});

describe('nextPromotionBoundary', () => {
  it('returns the earliest upcoming start or end', async () => {
    await promo({ startsAt: day(-1), endsAt: day(3) });            // ends in 3 days
    await promo({ startsAt: day(2), endsAt: day(5) });             // starts in 2 days
    await promo({ scope: 'product', categoryId: null, productId: bootId, startsAt: day(1), endsAt: day(9) }); // other category, product-scoped
    expect(await nextPromotionBoundary(db, { productId: beltId, categoryId: accessoriesId }, now)).toEqual(day(2));
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toEqual(day(2));
    expect(await nextPromotionBoundary(db, { categoryId: shoesId }, now)).toEqual(day(1));
    expect(await nextPromotionBoundary(db, { categoryId: null }, now)).toEqual(day(1));
  });
  it('returns null when nothing is scheduled', async () => {
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toBeNull();
  });
  it('ignores cancelled promotions', async () => {
    await promo({ startsAt: day(2), endsAt: day(5), cancelledAt: now });
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toBeNull();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm --filter @modaco/core test src/products`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement the queries**

`packages/core/src/products/queries.ts`:
```ts
import { sql, type SQL } from 'drizzle-orm';
import type { DbOrTx } from '../db/client';

export type SortDir = 'asc' | 'desc';

export interface ProductRecord {
  id: number;
  sku: string;
  name: string;
  category: { id: number; name: string; slug: string };
  basePrice: string;
  effectivePrice: string;
  activePromotion: { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string } | null;
}

/** Single source of truth for the effective price. `promo` is the lateral alias below. */
export const effectivePriceExpr: SQL = sql`
  case
    when promo.id is null then p.base_price
    when promo.discount_type = 'percentage' then round(p.base_price * (1 - promo.value / 100), 2)
    else greatest(p.base_price - promo.value, 0)
  end`;

function baseSelect(now: Date): SQL {
  const ts = now.toISOString();
  return sql`
    select p.id, p.sku, p.name, p.base_price,
           c.id as category_id, c.name as category_name, c.slug as category_slug,
           promo.id as promo_id, promo.name as promo_name, promo.discount_type as promo_discount_type, promo.value as promo_value,
           ${effectivePriceExpr} as effective_price
    from products p
    join categories c on c.id = p.category_id
    left join lateral (
      select pr.id, pr.name, pr.discount_type, pr.value
      from promotions pr
      where pr.cancelled_at is null
        and pr.starts_at <= ${ts}::timestamptz and ${ts}::timestamptz < pr.ends_at
        and ((pr.scope = 'product' and pr.product_id = p.id)
          or (pr.scope = 'category' and pr.category_id = p.category_id))
      order by pr.created_at desc, pr.id desc
      limit 1
    ) promo on true`;
}

function toRecord(r: Record<string, unknown>): ProductRecord {
  return {
    id: Number(r.id),
    sku: String(r.sku),
    name: String(r.name),
    category: { id: Number(r.category_id), name: String(r.category_name), slug: String(r.category_slug) },
    basePrice: String(r.base_price),
    effectivePrice: String(r.effective_price),
    activePromotion: r.promo_id
      ? { id: String(r.promo_id), name: String(r.promo_name), discountType: r.promo_discount_type as 'percentage' | 'fixed', value: String(r.promo_value) }
      : null,
  };
}

export async function fetchProductById(db: DbOrTx, id: number, now: Date): Promise<ProductRecord | null> {
  const res = await db.execute(sql`${baseSelect(now)} where p.id = ${id}`);
  const row = res.rows[0];
  return row ? toRecord(row) : null;
}

export async function fetchProductPage(
  db: DbOrTx,
  opts: { categoryId: number | null; sort: SortDir; limit: number; offset: number; now: Date },
): Promise<{ items: ProductRecord[]; total: number }> {
  const where = opts.categoryId === null ? sql`` : sql`where p.category_id = ${opts.categoryId}`;
  const dir = sql.raw(opts.sort === 'desc' ? 'desc' : 'asc');
  const [pageRes, countRes] = await Promise.all([
    db.execute(sql`${baseSelect(opts.now)} ${where} order by effective_price ${dir}, p.id ${dir} limit ${opts.limit} offset ${opts.offset}`),
    db.execute(sql`select count(*)::int as total from products p ${where}`),
  ]);
  return { items: pageRes.rows.map(toRecord), total: Number(countRes.rows[0]?.total ?? 0) };
}

export async function fetchStock(db: DbOrTx, ids: number[]): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  if (ids.length === 0) return out;
  const res = await db.execute(sql`select id, stock from products where id = any(${ids}::int[])`);
  for (const r of res.rows) out.set(Number(r.id), Number(r.stock));
  return out;
}

/**
 * Earliest future moment at which the effective price of anything in `scope` can change:
 * the next start of a scheduled promotion or the next end of an active one.
 */
export async function nextPromotionBoundary(
  db: DbOrTx,
  scope: { productId: number; categoryId: number } | { categoryId: number | null },
  now: Date,
): Promise<Date | null> {
  const ts = now.toISOString();
  let target: SQL;
  if ('productId' in scope) {
    target = sql`(pr.product_id = ${scope.productId} or pr.category_id = ${scope.categoryId})`;
  } else if (scope.categoryId === null) {
    target = sql`true`;
  } else {
    target = sql`(pr.category_id = ${scope.categoryId}
      or exists (select 1 from products p where p.id = pr.product_id and p.category_id = ${scope.categoryId}))`;
  }
  const res = await db.execute(sql`
    select min(b) as boundary from (
      select pr.starts_at as b from promotions pr
        where pr.cancelled_at is null and pr.starts_at > ${ts}::timestamptz and ${target}
      union all
      select pr.ends_at as b from promotions pr
        where pr.cancelled_at is null and pr.starts_at <= ${ts}::timestamptz and pr.ends_at > ${ts}::timestamptz and ${target}
    ) t`);
  const b = res.rows[0]?.boundary;
  return b ? new Date(b as string | Date) : null;
}
```

Add to `packages/core/src/index.ts`:
```ts
export * from './products/queries';
```

- [ ] **Step 4: Run tests**

Run: `pnpm --filter @modaco/core test src/products`
Expected: PASS, 12 tests. If `effectivePrice` comes back as `10` instead of `10.00`, the `round(..., 2)` branch is fine but the `greatest(...)` branch needs `::numeric(12,2)`: change it to `greatest(p.base_price - promo.value, 0)::numeric(12,2)` and re-run.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): effective price queries with lateral promotion resolution

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Cache keys, TTL rule, Redis client, versions, and read-through

**Files:**
- Create: `packages/core/src/cache/keys.ts`, `packages/core/src/cache/keys.test.ts`, `packages/core/src/cache/redis.ts`, `packages/core/src/cache/versions.ts`, `packages/core/src/cache/read-through.ts`, `packages/core/src/cache/read-through.test.ts`
- Modify: `packages/core/src/index.ts`

**Interfaces:**
- Produces:
  - `keys.productVersion(id)`, `keys.categoryVersion(id)`, `keys.allVersion()`, `keys.product(id)`, `keys.list(categoryId: number | null, sort: SortDir, page: number, pageSize: number)`, `keys.categorySlug(slug)`, `keys.stock(id)`, `keys.lock(key)`
  - `DEFAULT_TTL_SECONDS = 300`, `STOCK_TTL_SECONDS = 86400`, `SLUG_TTL_SECONDS = 300`
  - `ttlSeconds(now: Date, nextBoundary: Date | null, max?: number): number`
  - `createRedis(url: string): Redis` (ioredis instance, lazyConnect, fast-fail)
  - `parseVersion(raw: string | null): number`
  - `getVersions(redis, keys: string[]): Promise<number[]>`
  - `bumpVersions(redis, keys: string[], log?: (msg: string, err: unknown) => void): Promise<void>` (retries 3 times, never throws)
  - `bumpCategory(redis, categoryId, log?)` bumps category and all; `bumpProduct(redis, productId, log?)`
  - `readThrough<T>(redis: Redis | null, key: string, build: () => Promise<{ value: T; ttlSeconds: number }>, opts?: { isFresh?: (value: T) => Promise<boolean>; lockMs?: number; waitMs?: number; pollMs?: number; onError?: (err: unknown) => void }): Promise<{ value: T; source: 'hit' | 'built' | 'bypass' }>`

- [ ] **Step 1: Write the failing key and TTL tests**

`packages/core/src/cache/keys.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { DEFAULT_TTL_SECONDS, keys, ttlSeconds } from './keys';

describe('keys', () => {
  it('builds stable keys', () => {
    expect(keys.productVersion(7)).toBe('ver:product:7');
    expect(keys.categoryVersion(3)).toBe('ver:category:3');
    expect(keys.allVersion()).toBe('ver:all');
    expect(keys.product(7)).toBe('product:7');
    expect(keys.list(3, 'asc', 2, 20)).toBe('list:3:asc:2:20');
    expect(keys.list(null, 'desc', 1, 50)).toBe('list:all:desc:1:50');
    expect(keys.categorySlug('accessories')).toBe('category:slug:accessories');
    expect(keys.stock(7)).toBe('stock:7');
    expect(keys.lock('product:7')).toBe('lock:product:7');
  });
});

describe('ttlSeconds', () => {
  const now = new Date('2026-06-15T12:00:00Z');
  it('uses the default when nothing is scheduled', () => {
    expect(ttlSeconds(now, null)).toBe(DEFAULT_TTL_SECONDS);
  });
  it('caps at the next boundary, rounded up, minimum 1', () => {
    expect(ttlSeconds(now, new Date(now.getTime() + 90_500))).toBe(91);
    expect(ttlSeconds(now, new Date(now.getTime() + 10))).toBe(1);
    expect(ttlSeconds(now, new Date(now.getTime() - 1000))).toBe(1);
  });
  it('never exceeds the max', () => {
    expect(ttlSeconds(now, new Date(now.getTime() + 3_600_000))).toBe(DEFAULT_TTL_SECONDS);
    expect(ttlSeconds(now, null, 60)).toBe(60);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/core test src/cache/keys.test.ts`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement keys and TTL**

`packages/core/src/cache/keys.ts`:
```ts
import type { SortDir } from '../products/queries';

export const DEFAULT_TTL_SECONDS = 300;
export const STOCK_TTL_SECONDS = 86_400;
export const SLUG_TTL_SECONDS = 300;

export const keys = {
  productVersion: (id: number) => `ver:product:${id}`,
  categoryVersion: (id: number) => `ver:category:${id}`,
  allVersion: () => 'ver:all',
  product: (id: number) => `product:${id}`,
  list: (categoryId: number | null, sort: SortDir, page: number, pageSize: number) =>
    `list:${categoryId ?? 'all'}:${sort}:${page}:${pageSize}`,
  categorySlug: (slug: string) => `category:slug:${slug}`,
  stock: (id: number) => `stock:${id}`,
  lock: (key: string) => `lock:${key}`,
};

/** TTL is the default, capped so the entry expires exactly when the next promotion boundary would change a price. */
export function ttlSeconds(now: Date, nextBoundary: Date | null, max = DEFAULT_TTL_SECONDS): number {
  if (!nextBoundary) return max;
  const secs = Math.ceil((nextBoundary.getTime() - now.getTime()) / 1000);
  return Math.max(1, Math.min(max, secs));
}
```

- [ ] **Step 4: Redis client and versions**

`packages/core/src/cache/redis.ts`:
```ts
import Redis from 'ioredis';

/** Fast-failing client: a down Redis rejects commands immediately instead of queueing them. */
export function createRedis(url: string): Redis {
  return new Redis(url, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    connectTimeout: 2000,
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });
}

export type { Redis };
```

`packages/core/src/cache/versions.ts`:
```ts
import type Redis from 'ioredis';
import { keys } from './keys';

export function parseVersion(raw: string | null | undefined): number {
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

export async function getVersions(redis: Redis, versionKeys: string[]): Promise<number[]> {
  if (versionKeys.length === 0) return [];
  const raw = await redis.mget(...versionKeys);
  return raw.map(parseVersion);
}

type Log = (msg: string, err: unknown) => void;

/** Increment every key. Retries 3 times, then logs. Never throws: a missed bump self-heals via TTL. */
export async function bumpVersions(redis: Redis, versionKeys: string[], log: Log = () => {}): Promise<void> {
  if (versionKeys.length === 0) return;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const pipe = redis.pipeline();
      for (const k of versionKeys) pipe.incr(k);
      await pipe.exec();
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 50 * attempt));
    }
  }
  log(`version bump failed for ${versionKeys.join(',')}; stale until TTL`, lastErr);
}

export function bumpCategory(redis: Redis, categoryId: number, log?: Log): Promise<void> {
  return bumpVersions(redis, [keys.categoryVersion(categoryId), keys.allVersion()], log);
}

export function bumpProduct(redis: Redis, productId: number, log?: Log): Promise<void> {
  return bumpVersions(redis, [keys.productVersion(productId)], log);
}
```

- [ ] **Step 5: Write the failing read-through test**

`packages/core/src/cache/read-through.test.ts`:
```ts
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createRedis } from './redis';
import { readThrough } from './read-through';

const redis = createRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
beforeAll(() => redis.connect());
beforeEach(() => redis.flushdb());
afterAll(() => redis.quit());

describe('readThrough', () => {
  it('builds on miss, hits afterwards, and honours the TTL', async () => {
    let builds = 0;
    const build = async () => { builds++; return { value: { n: 1 }, ttlSeconds: 30 }; };
    const a = await readThrough(redis, 'k', build);
    expect(a).toEqual({ value: { n: 1 }, source: 'built' });
    const b = await readThrough(redis, 'k', build);
    expect(b).toEqual({ value: { n: 1 }, source: 'hit' });
    expect(builds).toBe(1);
    expect(await redis.ttl('k')).toBeGreaterThan(25);
  });

  it('treats a stale entry as a miss and overwrites it', async () => {
    await redis.set('k', JSON.stringify({ v: 1 }));
    const res = await readThrough(redis, 'k', async () => ({ value: { v: 2 }, ttlSeconds: 10 }), {
      isFresh: async (value) => (value as { v: number }).v === 2,
    });
    expect(res).toEqual({ value: { v: 2 }, source: 'built' });
    expect(JSON.parse((await redis.get('k'))!)).toEqual({ v: 2 });
  });

  it('coalesces concurrent misses into one build', async () => {
    let builds = 0;
    const build = async () => {
      builds++;
      await new Promise((r) => setTimeout(r, 100));
      return { value: 'x', ttlSeconds: 10 };
    };
    const results = await Promise.all(Array.from({ length: 8 }, () => readThrough(redis, 'k', build)));
    expect(results.every((r) => r.value === 'x')).toBe(true);
    expect(builds).toBe(1);
    expect(results.filter((r) => r.source === 'built')).toHaveLength(1);
    expect(results.filter((r) => r.source === 'hit')).toHaveLength(7);
  });

  it('bypasses the cache when the lock holder is slow', async () => {
    await redis.set('lock:k', '1', 'PX', 5000);
    const res = await readThrough(redis, 'k', async () => ({ value: 'y', ttlSeconds: 10 }), { waitMs: 50, pollMs: 10 });
    expect(res).toEqual({ value: 'y', source: 'bypass' });
    expect(await redis.get('k')).toBeNull();
  });

  it('falls through to the builder when redis is unavailable', async () => {
    const dead = createRedis('redis://localhost:1');
    dead.on('error', () => {}); // ioredis emits 'error' events; unhandled ones would throw
    const errors: unknown[] = [];
    const res = await readThrough(dead, 'k', async () => ({ value: 'z', ttlSeconds: 10 }), { onError: (e) => errors.push(e) });
    expect(res).toEqual({ value: 'z', source: 'bypass' });
    expect(errors.length).toBeGreaterThan(0);
    dead.disconnect();
  });

  it('works with a null redis', async () => {
    expect(await readThrough(null, 'k', async () => ({ value: 1, ttlSeconds: 1 }))).toEqual({ value: 1, source: 'bypass' });
  });
});
```

- [ ] **Step 6: Run to verify failure**

Run: `pnpm --filter @modaco/core test src/cache/read-through.test.ts`
Expected: FAIL, module not found.

- [ ] **Step 7: Implement read-through**

`packages/core/src/cache/read-through.ts`:
```ts
import type Redis from 'ioredis';
import { keys } from './keys';

export interface ReadThroughOptions<T> {
  isFresh?: (value: T) => Promise<boolean>;
  lockMs?: number;
  waitMs?: number;
  pollMs?: number;
  onError?: (err: unknown) => void;
}

export interface CacheOutcome<T> {
  value: T;
  source: 'hit' | 'built' | 'bypass';
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Cache-aside read with request coalescing. Any Redis failure degrades to a direct build.
 * `isFresh` lets the caller reject an entry whose embedded version numbers are stale.
 */
export async function readThrough<T>(
  redis: Redis | null,
  key: string,
  build: () => Promise<{ value: T; ttlSeconds: number }>,
  opts: ReadThroughOptions<T> = {},
): Promise<CacheOutcome<T>> {
  const { lockMs = 2000, waitMs = 200, pollMs = 20, onError = () => {} } = opts;
  if (!redis) return { value: (await build()).value, source: 'bypass' };

  const tryHit = async (): Promise<T | undefined> => {
    const raw = await redis.get(key);
    if (raw === null) return undefined;
    const value = JSON.parse(raw) as T;
    if (opts.isFresh && !(await opts.isFresh(value))) return undefined;
    return value;
  };

  try {
    const hit = await tryHit();
    if (hit !== undefined) return { value: hit, source: 'hit' };

    const lockKey = keys.lock(key);
    const locked = await redis.set(lockKey, '1', 'PX', lockMs, 'NX');
    if (locked === 'OK') {
      try {
        const built = await build();
        await redis.set(key, JSON.stringify(built.value), 'EX', Math.max(1, built.ttlSeconds));
        return { value: built.value, source: 'built' };
      } finally {
        await redis.del(lockKey).catch(onError);
      }
    }

    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      await sleep(pollMs);
      const late = await tryHit();
      if (late !== undefined) return { value: late, source: 'hit' };
    }
  } catch (err) {
    onError(err);
  }
  return { value: (await build()).value, source: 'bypass' };
}
```

Add to `packages/core/src/index.ts`:
```ts
export * from './cache/keys';
export * from './cache/redis';
export * from './cache/versions';
export * from './cache/read-through';
```

- [ ] **Step 8: Run all core tests**

Run: `pnpm --filter @modaco/core test`
Expected: PASS. The "unavailable" test may take ~2 s while the connect times out.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(core): cache keys, ttl rule, version counters, coalescing read-through

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Byte-range chunking and line ownership

**Files:**
- Create: `packages/core/src/ingest/chunking.ts`, `packages/core/src/ingest/chunking.test.ts`
- Modify: `packages/core/src/index.ts`

**Interfaces:**
- Produces:
  - `interface ChunkRange { chunkIndex: number; byteStart: number; byteEnd: number }` (`byteEnd` inclusive)
  - `computeChunks(contentLength: number, chunkSizeBytes: number): ChunkRange[]`
  - `OVERREAD_BYTES = 65536`, `MAX_LINE_BYTES = 65536`
  - `rangeFor(chunk: { byteStart: number; byteEnd: number }): { rangeStart: number; rangeEnd: number }` (`rangeStart = max(byteStart - 1, 0)`, `rangeEnd = byteEnd + OVERREAD_BYTES`)
  - `ownedLines(source: AsyncIterable<Uint8Array>, chunk: { byteStart: number; byteEnd: number }, rangeStart: number): AsyncGenerator<{ line: string; offset: number }>`

The ownership rule: a line belongs to the chunk whose range contains the line's first byte. Reading from `byteStart - 1` lets the generator see whether the byte before the chunk is a newline, which decides whether the segment starting at `byteStart` is a fresh line (owned) or the tail of a line owned by the previous chunk (skipped). This replaces the naive "discard through the first newline" rule, which drops a line when a chunk boundary falls exactly after a newline.

- [ ] **Step 1: Write the failing chunking tests**

`packages/core/src/ingest/chunking.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { computeChunks, MAX_LINE_BYTES, ownedLines, rangeFor } from './chunking';

async function* pieces(buf: Buffer, size: number): AsyncGenerator<Uint8Array> {
  for (let i = 0; i < buf.length; i += size) yield buf.subarray(i, Math.min(i + size, buf.length));
}

async function collectAll(buf: Buffer, chunkSize: number, pieceSize: number): Promise<string[]> {
  const lines: string[] = [];
  for (const chunk of computeChunks(buf.length, chunkSize)) {
    const { rangeStart, rangeEnd } = rangeFor(chunk);
    const slice = buf.subarray(rangeStart, Math.min(rangeEnd + 1, buf.length));
    for await (const { line } of ownedLines(pieces(slice, pieceSize), chunk, rangeStart)) lines.push(line);
  }
  return lines;
}

describe('computeChunks', () => {
  it('splits a length into inclusive byte ranges', () => {
    expect(computeChunks(10, 4)).toEqual([
      { chunkIndex: 0, byteStart: 0, byteEnd: 3 },
      { chunkIndex: 1, byteStart: 4, byteEnd: 7 },
      { chunkIndex: 2, byteStart: 8, byteEnd: 9 },
    ]);
    expect(computeChunks(8, 4)).toHaveLength(2);
    expect(computeChunks(0, 4)).toEqual([]);
    expect(computeChunks(3, 4)).toEqual([{ chunkIndex: 0, byteStart: 0, byteEnd: 2 }]);
  });
  it('rangeFor reads one byte early and over-reads past the end', () => {
    expect(rangeFor({ byteStart: 0, byteEnd: 3 })).toEqual({ rangeStart: 0, rangeEnd: 3 + 65536 });
    expect(rangeFor({ byteStart: 4, byteEnd: 7 })).toEqual({ rangeStart: 3, rangeEnd: 7 + 65536 });
  });
});

describe('ownedLines', () => {
  const text = ['sku,name', 'A,alpha', 'BB,beta', 'C,c', 'DDDD,delta delta', 'E,e', 'F,f'].join('\n') + '\n';
  const expected = text.trimEnd().split('\n');

  it('yields every line exactly once for any chunk size and piece size', async () => {
    const buf = Buffer.from(text);
    for (const chunkSize of [1, 2, 3, 5, 7, 8, 11, 16, 64, 1000]) {
      for (const pieceSize of [1, 3, 1000]) {
        expect(await collectAll(buf, chunkSize, pieceSize), `chunk=${chunkSize} piece=${pieceSize}`).toEqual(expected);
      }
    }
  });

  it('handles a boundary exactly after a newline', async () => {
    const buf = Buffer.from('ab\ncd\nef\n'); // newline at index 2; chunk 1 starts at 3
    const lines = await collectAll(buf, 3, 100);
    expect(lines).toEqual(['ab', 'cd', 'ef']);
  });

  it('handles a file without a trailing newline and CRLF endings', async () => {
    expect(await collectAll(Buffer.from('a\r\nb\r\nc'), 2, 1)).toEqual(['a', 'b', 'c']);
    expect(await collectAll(Buffer.from('a\nb\nc'), 100, 100)).toEqual(['a', 'b', 'c']);
  });

  it('reports absolute offsets', async () => {
    const buf = Buffer.from('ab\ncd\nef\n');
    const chunk = { byteStart: 3, byteEnd: 5 };
    const { rangeStart } = rangeFor(chunk);
    const out: number[] = [];
    for await (const { offset } of ownedLines(pieces(buf.subarray(rangeStart), 100), chunk, rangeStart)) out.push(offset);
    expect(out).toEqual([3]);
  });

  it('rejects a line longer than the limit', async () => {
    const buf = Buffer.from('x'.repeat(MAX_LINE_BYTES + 10) + '\n');
    const chunk = { byteStart: 0, byteEnd: buf.length - 1 };
    await expect(async () => { for await (const _ of ownedLines(pieces(buf, 4096), chunk, 0)) { /* drain */ } }).rejects.toThrow(/exceeds/);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/core test src/ingest/chunking.test.ts`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement chunking**

`packages/core/src/ingest/chunking.ts`:
```ts
export interface ChunkRange { chunkIndex: number; byteStart: number; byteEnd: number }

export const OVERREAD_BYTES = 65_536;
export const MAX_LINE_BYTES = 65_536;

/** Fixed-size inclusive byte ranges covering [0, contentLength). Pure arithmetic: never reads the file. */
export function computeChunks(contentLength: number, chunkSizeBytes: number): ChunkRange[] {
  if (chunkSizeBytes <= 0) throw new Error('chunkSizeBytes must be positive');
  const out: ChunkRange[] = [];
  for (let start = 0, i = 0; start < contentLength; start += chunkSizeBytes, i++) {
    out.push({ chunkIndex: i, byteStart: start, byteEnd: Math.min(start + chunkSizeBytes, contentLength) - 1 });
  }
  return out;
}

/** The S3 range to request for a chunk: one byte early (to see the preceding newline) and an over-read to finish the last line. */
export function rangeFor(chunk: { byteStart: number; byteEnd: number }): { rangeStart: number; rangeEnd: number } {
  return { rangeStart: Math.max(chunk.byteStart - 1, 0), rangeEnd: chunk.byteEnd + OVERREAD_BYTES };
}

/**
 * Yields the lines owned by `chunk`: those whose first byte lies in [byteStart, byteEnd].
 * `rangeStart` is the absolute offset of the first byte of `source`.
 */
export async function* ownedLines(
  source: AsyncIterable<Uint8Array>,
  chunk: { byteStart: number; byteEnd: number },
  rangeStart: number,
): AsyncGenerator<{ line: string; offset: number }> {
  let buffered = Buffer.alloc(0);
  let segmentStart = rangeStart; // absolute offset of buffered[0]

  const toLine = (b: Buffer) => (b.length && b[b.length - 1] === 13 ? b.subarray(0, -1) : b).toString('utf8');

  for await (const piece of source) {
    buffered = buffered.length ? Buffer.concat([buffered, piece]) : Buffer.from(piece);
    let nl: number;
    while ((nl = buffered.indexOf(10)) !== -1) {
      const offset = segmentStart;
      const lineBuf = buffered.subarray(0, nl);
      buffered = buffered.subarray(nl + 1);
      segmentStart = offset + nl + 1;
      if (offset > chunk.byteEnd) return;
      if (offset >= chunk.byteStart) yield { line: toLine(lineBuf), offset };
    }
    if (segmentStart > chunk.byteEnd) return;
    if (buffered.length > MAX_LINE_BYTES) throw new Error(`line at byte ${segmentStart} exceeds ${MAX_LINE_BYTES} bytes`);
  }
  if (buffered.length > 0 && segmentStart >= chunk.byteStart && segmentStart <= chunk.byteEnd) {
    yield { line: toLine(buffered), offset: segmentStart };
  }
}
```

Add to `packages/core/src/index.ts`:
```ts
export * from './ingest/chunking';
```

- [ ] **Step 4: Run tests**

Run: `pnpm --filter @modaco/core test src/ingest`
Expected: PASS, all chunking and csv tests.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): byte-range chunk computation and line ownership stream

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: API skeleton: config, errors, middleware, health

**Files:**
- Create: `apps/api/package.json`, `apps/api/tsconfig.json`, `apps/api/vitest.config.ts`, `apps/api/src/config.ts`, `apps/api/src/logger.ts`, `apps/api/src/errors.ts`, `apps/api/src/middleware/request-id.ts`, `apps/api/src/middleware/validate.ts`, `apps/api/src/middleware/error-handler.ts`, `apps/api/src/deps.ts`, `apps/api/src/app.ts`, `apps/api/src/server.ts`, `apps/api/src/routes/health.ts`, `apps/api/test/helpers.ts`, `apps/api/test/health.test.ts`

**Interfaces:**
- Produces:
  - `loadConfig(env = process.env): Config` with fields `port, databaseUrl, redisUrl, awsRegion, awsEndpointUrl, s3PublicEndpoint, s3Bucket, logLevel`
  - `class HttpError extends Error { status: number; code: string; details?: unknown }`, helpers `notFound(message)`, `unprocessable(message, details?)`, `conflict(message)`
  - `validate({ body?, query?, params? })` middleware storing parsed values in `res.locals.input`
  - `input<B, Q, P>(res): { body: B; query: Q; params: P }`
  - `interface AppDeps { db: Db; redis: Redis | null; s3: S3Client; presigner: S3Client; config: Config; logger: Logger; now: () => Date }`
  - `createDeps(config): Promise<AppDeps & { close(): Promise<void> }>`
  - `createApp(deps: AppDeps): Express`
  - Test helpers: `setupTestDeps(): Promise<TestContext>` where `TestContext = { deps: AppDeps; app: Express; db: Db; redis: Redis; truncateAll(): Promise<void>; insertCategory(name, over?): Promise<{ id: number; slug: string }>; insertProduct(over): Promise<{ id: number }>; close(): Promise<void> }`

- [ ] **Step 1: Package files**

`apps/api/package.json`:
```json
{
  "name": "@modaco/api",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "tsx watch src/server.ts",
    "start": "tsx src/server.ts",
    "test": "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-s3": "^3.700.0",
    "@aws-sdk/s3-request-presigner": "^3.700.0",
    "@modaco/core": "workspace:*",
    "drizzle-orm": "^0.44.2",
    "express": "^5.1.0",
    "ioredis": "^5.4.1",
    "pino": "^9.5.0",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@types/express": "^5.0.0",
    "@types/supertest": "^6.0.2",
    "supertest": "^7.0.0",
    "tsx": "^4.19.1",
    "typescript": "^5.6.3",
    "vitest": "^3.0.5"
  }
}
```

`apps/api/tsconfig.json`:
```json
{ "extends": "../../tsconfig.base.json", "include": ["src", "test"] }
```

`apps/api/vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';
export default defineConfig({ test: { include: ['test/**/*.test.ts'], testTimeout: 30000, hookTimeout: 30000, fileParallelism: false } });
```

- [ ] **Step 2: Config, logger, errors**

`apps/api/src/config.ts`:
```ts
import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().default(3000),
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  AWS_ENDPOINT_URL: z.string().default('http://localhost:4566'),
  S3_PUBLIC_ENDPOINT: z.string().default('http://localhost:4566'),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  LOG_LEVEL: z.string().default('info'),
});

export interface Config {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  awsRegion: string;
  awsEndpointUrl: string;
  s3PublicEndpoint: string;
  s3Bucket: string;
  logLevel: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    port: e.PORT, databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION,
    awsEndpointUrl: e.AWS_ENDPOINT_URL, s3PublicEndpoint: e.S3_PUBLIC_ENDPOINT, s3Bucket: e.S3_BUCKET, logLevel: e.LOG_LEVEL,
  };
}
```

`apps/api/src/logger.ts`:
```ts
import pino, { type Logger } from 'pino';
export type { Logger };
export function createLogger(level: string): Logger {
  return pino({ level });
}
```

`apps/api/src/errors.ts`:
```ts
export class HttpError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string, public readonly details?: unknown) {
    super(message);
  }
}
export const notFound = (message: string) => new HttpError(404, 'not_found', message);
export const unprocessable = (message: string, details?: unknown) => new HttpError(422, 'unprocessable', message, details);
export const conflict = (message: string) => new HttpError(409, 'conflict', message);

// drizzle-orm 0.44 wraps driver errors in DrizzleQueryError; the pg SQLSTATE or
// errno code lives on `cause`. Check both so raw pg errors keep working.
export function pgErrorCode(err: unknown): string | undefined {
  const e = err as { code?: unknown; cause?: { code?: unknown } } | undefined;
  const code = e?.code ?? e?.cause?.code;
  return typeof code === 'string' ? code : undefined;
}
```

- [ ] **Step 3: Middleware**

`apps/api/src/middleware/request-id.ts`:
```ts
import type { RequestHandler } from 'express';
import { randomUUID } from 'node:crypto';

export const requestId: RequestHandler = (req, res, next) => {
  const id = (req.header('x-request-id') ?? randomUUID()).slice(0, 64);
  res.locals.requestId = id;
  res.setHeader('x-request-id', id);
  next();
};
```

`apps/api/src/middleware/validate.ts`:
```ts
import type { RequestHandler, Response } from 'express';
import type { ZodTypeAny } from 'zod';
import { HttpError } from '../errors';

interface Schemas { body?: ZodTypeAny; query?: ZodTypeAny; params?: ZodTypeAny }

export function validate(schemas: Schemas): RequestHandler {
  return (req, res, next) => {
    const parsed: Record<string, unknown> = {};
    for (const part of ['params', 'query', 'body'] as const) {
      const schema = schemas[part];
      if (!schema) continue;
      const result = schema.safeParse(req[part]);
      if (!result.success) {
        const details = result.error.issues.map((i) => ({ path: [part, ...i.path].join('.'), message: i.message }));
        return next(new HttpError(400, 'validation_error', `invalid ${part}`, details));
      }
      parsed[part] = result.data;
    }
    res.locals.input = parsed;
    next();
  };
}

export function input<B = unknown, Q = unknown, P = unknown>(res: Response): { body: B; query: Q; params: P } {
  return res.locals.input as { body: B; query: Q; params: P };
}
```

`apps/api/src/middleware/error-handler.ts`:
```ts
import type { ErrorRequestHandler } from 'express';
import type { Logger } from '../logger';
import { HttpError, pgErrorCode } from '../errors';

const DB_UNAVAILABLE = new Set(['ECONNREFUSED', 'ETIMEDOUT', '57P01', '57P02', '57P03', '08006', '08001']);

export function errorHandler(logger: Logger): ErrorRequestHandler {
  return (err, _req, res, _next) => {
    const requestId = res.locals.requestId as string | undefined;
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message, details: err.details } });
      return;
    }
    const code = pgErrorCode(err);
    if (code && DB_UNAVAILABLE.has(code)) {
      logger.error({ err, requestId }, 'database unavailable');
      res.status(503).json({ error: { code: 'database_unavailable', message: 'database unavailable' } });
      return;
    }
    if (err?.type === 'entity.parse.failed') {
      res.status(400).json({ error: { code: 'validation_error', message: 'malformed JSON body' } });
      return;
    }
    logger.error({ err, requestId }, 'unhandled error');
    res.status(500).json({ error: { code: 'internal_error', message: 'internal error', requestId } });
  };
}
```

- [ ] **Step 4: Deps, app, server, health**

`apps/api/src/deps.ts`:
```ts
import { S3Client } from '@aws-sdk/client-s3';
import { createDb, createRedis, type Db, type Redis } from '@modaco/core';
import type { Config } from './config';
import { createLogger, type Logger } from './logger';

export interface AppDeps {
  db: Db;
  redis: Redis | null;
  s3: S3Client;
  presigner: S3Client;
  config: Config;
  logger: Logger;
  now: () => Date;
}

export async function createDeps(config: Config): Promise<AppDeps & { close(): Promise<void> }> {
  const logger = createLogger(config.logLevel);
  const { db, close: closeDb } = createDb(config.databaseUrl, { max: 10 });
  const redis = createRedis(config.redisUrl);
  redis.on('error', (err) => logger.warn({ err }, 'redis error'));
  await redis.connect().catch((err) => logger.warn({ err }, 'redis initial connect failed; continuing degraded'));
  const s3Opts = { region: config.awsRegion, forcePathStyle: true, credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' } };
  const s3 = new S3Client({ ...s3Opts, endpoint: config.awsEndpointUrl });
  const presigner = new S3Client({ ...s3Opts, endpoint: config.s3PublicEndpoint });
  return {
    db, redis, s3, presigner, config, logger, now: () => new Date(),
    close: async () => { await closeDb(); redis.disconnect(); },
  };
}
```

`apps/api/src/routes/health.ts`:
```ts
import { Router } from 'express';
import { sql } from 'drizzle-orm';
import type { AppDeps } from '../deps';

export function healthRoutes(deps: AppDeps): Router {
  const r = Router();
  r.get('/health', async (_req, res) => {
    const checks = { postgres: false, redis: false };
    try { await deps.db.execute(sql`select 1`); checks.postgres = true; } catch { /* reported below */ }
    try { if (deps.redis && (await deps.redis.ping()) === 'PONG') checks.redis = true; } catch { /* reported below */ }
    res.status(checks.postgres ? 200 : 503).json({ status: checks.postgres ? 'ok' : 'degraded', checks });
  });
  return r;
}
```

`apps/api/src/app.ts`:
```ts
import express, { type Express } from 'express';
import type { AppDeps } from './deps';
import { errorHandler } from './middleware/error-handler';
import { requestId } from './middleware/request-id';
import { healthRoutes } from './routes/health';

export function createApp(deps: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.use(requestId);
  app.use(express.json({ limit: '1mb' }));
  app.use(healthRoutes(deps));
  app.use((_req, res) => res.status(404).json({ error: { code: 'not_found', message: 'route not found' } }));
  app.use(errorHandler(deps.logger));
  return app;
}
```

`apps/api/src/server.ts`:
```ts
import { createApp } from './app';
import { loadConfig } from './config';
import { createDeps } from './deps';

const config = loadConfig();
const deps = await createDeps(config);
const app = createApp(deps);
const server = app.listen(config.port, () => deps.logger.info({ port: config.port }, 'api listening'));

const shutdown = async () => {
  server.close();
  await deps.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
```

- [ ] **Step 5: Test helpers and health test**

`apps/api/test/helpers.ts`:
```ts
import { sql } from 'drizzle-orm';
import type { Express } from 'express';
import { categories, products, runMigrations, slugify, type Db, type Redis } from '@modaco/core';
import { createApp } from '../src/app';
import { loadConfig } from '../src/config';
import { createDeps, type AppDeps } from '../src/deps';

export interface TestContext {
  deps: AppDeps;
  app: Express;
  db: Db;
  redis: Redis;
  truncateAll(): Promise<void>;
  insertCategory(name: string, over?: Partial<typeof categories.$inferInsert>): Promise<{ id: number; slug: string }>;
  insertProduct(over: Partial<typeof products.$inferInsert> & { categoryId: number }): Promise<{ id: number }>;
  close(): Promise<void>;
}

export async function setupTestDeps(): Promise<TestContext> {
  const config = loadConfig({ ...process.env, LOG_LEVEL: 'silent' });
  const real = await createDeps(config);
  const deps: AppDeps = { ...real };
  await runMigrations(deps.db);
  const redis = deps.redis!;
  let counter = 0;
  return {
    deps,
    app: createApp(deps),
    db: deps.db,
    redis,
    truncateAll: async () => {
      await deps.db.execute(sql`truncate ingestion_rejections, ingestion_chunks, ingestion_jobs, promotions, products, categories restart identity cascade`);
      await redis.flushdb();
    },
    insertCategory: async (name, over = {}) => {
      const [row] = await deps.db.insert(categories).values({ name, slug: slugify(name), ...over }).returning();
      return { id: row!.id, slug: row!.slug };
    },
    insertProduct: async (over) => {
      counter++;
      const [row] = await deps.db.insert(products).values({ sku: `SKU-${counter}`, name: `Product ${counter}`, basePrice: '10.00', stock: 5, ...over }).returning();
      return { id: row!.id };
    },
    close: () => real.close(),
  };
}
```

`apps/api/test/health.test.ts`:
```ts
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());

describe('GET /health', () => {
  it('reports postgres and redis', async () => {
    const res = await request(ctx.app).get('/health');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: 'ok', checks: { postgres: true, redis: true } });
    expect(res.headers['x-request-id']).toBeTruthy();
  });
  it('returns the error envelope for unknown routes', async () => {
    const res = await request(ctx.app).get('/nope');
    expect(res.status).toBe(404);
    expect(res.body.error.code).toBe('not_found');
  });
});
```

- [ ] **Step 6: Run**

Run: `pnpm install && pnpm --filter @modaco/api test`
Expected: PASS, 2 tests.

Run: `pnpm --filter @modaco/api typecheck`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(api): express skeleton with config, validation, error envelope, health

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Product read endpoints with version-validated cache

**Files:**
- Create: `apps/api/src/products/schemas.ts`, `apps/api/src/products/stock.ts`, `apps/api/src/products/cached-reads.ts`, `apps/api/src/products/service.ts`, `apps/api/src/products/routes.ts`, `apps/api/test/products.test.ts`
- Modify: `apps/api/src/app.ts`

**Interfaces:**
- Produces:
  - `interface ProductItem extends ProductRecord { stock: number }`
  - `loadStocks(deps, ids: number[]): Promise<Map<number, number>>`
  - `setStock(deps, id: number, stock: number): Promise<void>` (Redis set with `STOCK_TTL_SECONDS`, swallows errors)
  - `resolveCategoryId(deps, slug: string): Promise<number | null>`
  - `getCachedProduct(deps, id: number): Promise<ProductRecord | null>`
  - `getCachedProductPage(deps, opts: { categoryId: number | null; sort: SortDir; page: number; pageSize: number }): Promise<{ items: ProductRecord[]; total: number }>`
  - `class ProductService { constructor(deps); getProduct(id): Promise<ProductItem | null>; listProducts(q: { category?: string; sort: 'effective_price' | '-effective_price'; page: number; pageSize: number }): Promise<{ items: ProductItem[]; pagination: { page: number; pageSize: number; total: number } }> }` (throws `notFound` for an unknown category slug)
  - `productRoutes(deps): Router`

- [ ] **Step 1: Write the failing product read tests**

`apps/api/test/products.test.ts`:
```ts
import { keys } from '@modaco/core';
import { promotions } from '@modaco/core';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
let acc: { id: number; slug: string };
let shoes: { id: number; slug: string };
let belt: { id: number };
let hat: { id: number };

beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  acc = await ctx.insertCategory('Accessories');
  shoes = await ctx.insertCategory('Shoes');
  belt = await ctx.insertProduct({ categoryId: acc.id, sku: 'BELT', name: 'Belt', basePrice: '20.00', stock: 3 });
  hat = await ctx.insertProduct({ categoryId: acc.id, sku: 'HAT', name: 'Hat', basePrice: '10.00', stock: 0 });
  await ctx.insertProduct({ categoryId: shoes.id, sku: 'BOOT', name: 'Boot', basePrice: '100.00', stock: 7 });
});

const activePromo = (over: Partial<typeof promotions.$inferInsert> = {}) => ctx.db.insert(promotions).values({
  name: 'Sale', discountType: 'percentage', value: '50', scope: 'category', categoryId: acc.id,
  startsAt: new Date(Date.now() - 60_000), endsAt: new Date(Date.now() + 3_600_000), ...over,
}).returning();

describe('GET /products/:id', () => {
  it('returns the item shape with effective price and stock', async () => {
    const res = await request(ctx.app).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      id: belt.id, sku: 'BELT', name: 'Belt', category: { id: acc.id, name: 'Accessories', slug: 'accessories' },
      basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, stock: 3,
    });
  });

  it('404s for an unknown id and 400s for a bad id', async () => {
    expect((await request(ctx.app).get('/products/999999')).status).toBe(404);
    expect((await request(ctx.app).get('/products/abc')).status).toBe(400);
  });

  it('serves from cache and invalidates when the category version is bumped', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    expect(await ctx.redis.exists(keys.product(belt.id))).toBe(1);
    await activePromo();
    // no bump yet: cached price still 20.00
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    await ctx.redis.incr(keys.categoryVersion(acc.id));
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });

  it('reads stock live while the catalog entry stays cached', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    await ctx.redis.set(keys.stock(belt.id), '42');
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(42);
  });

  it('caps the TTL at the next promotion boundary', async () => {
    await activePromo({ startsAt: new Date(Date.now() + 30_000), endsAt: new Date(Date.now() + 60_000) });
    await request(ctx.app).get(`/products/${belt.id}`);
    const ttl = await ctx.redis.ttl(keys.product(belt.id));
    expect(ttl).toBeGreaterThan(20);
    expect(ttl).toBeLessThanOrEqual(31);
  });

  it('still answers when redis is down', async () => {
    const dead = { ...ctx.deps, redis: null };
    const { createApp } = await import('../src/app');
    const res = await request(createApp(dead)).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body.stock).toBe(3);
  });
});

describe('GET /products', () => {
  it('filters, sorts, and paginates', async () => {
    await activePromo({ scope: 'product', categoryId: null, productId: belt.id, value: '90' }); // belt -> 2.00
    const asc = await request(ctx.app).get(`/products?category=accessories&sort=effective_price`);
    expect(asc.status).toBe(200);
    expect(asc.body.items.map((i: { sku: string }) => i.sku)).toEqual(['BELT', 'HAT']);
    expect(asc.body.pagination).toEqual({ page: 1, pageSize: 20, total: 2 });
    const desc = await request(ctx.app).get(`/products?category=accessories&sort=-effective_price&pageSize=1&page=2`);
    expect(desc.body.items.map((i: { sku: string }) => i.sku)).toEqual(['BELT']);
    expect(desc.body.pagination).toEqual({ page: 2, pageSize: 1, total: 2 });
    const all = await request(ctx.app).get('/products');
    expect(all.body.pagination.total).toBe(3);
    expect(all.body.items[0]).toHaveProperty('stock');
  });

  it('validates query params', async () => {
    expect((await request(ctx.app).get('/products?pageSize=500')).status).toBe(400);
    expect((await request(ctx.app).get('/products?sort=name')).status).toBe(400);
    expect((await request(ctx.app).get('/products?category=nope')).status).toBe(404);
  });

  it('caches a page and rebuilds it after a version bump', async () => {
    await request(ctx.app).get('/products?category=accessories');
    expect(await ctx.redis.exists(keys.list(acc.id, 'asc', 1, 20))).toBe(1);
    await activePromo();
    expect((await request(ctx.app).get('/products?category=accessories')).body.items[0].effectivePrice).toBe('10.00'); // HAT cached at 10.00
    await ctx.redis.incr(keys.categoryVersion(acc.id));
    const after = await request(ctx.app).get('/products?category=accessories');
    expect(after.body.items.map((i: { effectivePrice: string }) => i.effectivePrice)).toEqual(['5.00', '10.00']);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/api test test/products.test.ts`
Expected: FAIL, routes return 404.

- [ ] **Step 3: Implement schemas, stock, cached reads**

`apps/api/src/products/schemas.ts`:
```ts
import { z } from 'zod';

export const idParam = z.object({ id: z.coerce.number().int().positive() });

export const listQuery = z.object({
  category: z.string().min(1).max(100).optional(),
  sort: z.enum(['effective_price', '-effective_price']).default('effective_price'),
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(20),
});
export type ListQuery = z.infer<typeof listQuery>;

export const createProductBody = z.object({
  sku: z.string().trim().min(1).max(64),
  name: z.string().trim().min(1).max(255),
  categoryId: z.number().int().positive(),
  basePrice: z.string().regex(/^\d+(\.\d{1,2})?$/, 'basePrice must be a decimal with up to 2 places'),
  stock: z.number().int().min(0).default(0),
});
export type CreateProductBody = z.infer<typeof createProductBody>;

export const stockBody = z.union([
  z.object({ delta: z.number().int() }),
  z.object({ stock: z.number().int().min(0) }),
]);
export type StockBody = z.infer<typeof stockBody>;
```

`apps/api/src/products/stock.ts`:
```ts
import { fetchStock, keys, STOCK_TTL_SECONDS } from '@modaco/core';
import type { AppDeps } from '../deps';

/** Redis counters first, Postgres for misses, counters backfilled. Redis failure means Postgres only. */
export async function loadStocks(deps: AppDeps, ids: number[]): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  if (ids.length === 0) return out;
  let missing = ids;
  if (deps.redis) {
    try {
      const raw = await deps.redis.mget(...ids.map(keys.stock));
      missing = [];
      ids.forEach((id, i) => {
        const v = raw[i];
        if (v === null || v === undefined) missing.push(id); else out.set(id, Number(v));
      });
    } catch (err) {
      deps.logger.warn({ err }, 'stock mget failed; falling back to postgres');
      missing = ids;
    }
  }
  if (missing.length > 0) {
    const fromDb = await fetchStock(deps.db, missing);
    for (const [id, stock] of fromDb) out.set(id, stock);
    if (deps.redis && fromDb.size > 0) {
      const pipe = deps.redis.pipeline();
      for (const [id, stock] of fromDb) pipe.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS);
      await pipe.exec().catch((err) => deps.logger.warn({ err }, 'stock backfill failed'));
    }
  }
  return out;
}

export async function setStock(deps: AppDeps, id: number, stock: number): Promise<void> {
  if (!deps.redis) return;
  await deps.redis.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS)
    .catch((err) => deps.logger.warn({ err, id }, 'stock counter set failed'));
}
```

`apps/api/src/products/cached-reads.ts`:
```ts
import { eq } from 'drizzle-orm';
import {
  categories, fetchProductById, fetchProductPage, getVersions, keys, nextPromotionBoundary, readThrough,
  SLUG_TTL_SECONDS, ttlSeconds, type ProductRecord, type SortDir,
} from '@modaco/core';
import type { AppDeps } from '../deps';

interface ProductEntry { productVersion: number; categoryVersion: number; categoryId: number; record: ProductRecord }
interface PageEntry { version: number; items: ProductRecord[]; total: number }

const onError = (deps: AppDeps) => (err: unknown) => deps.logger.warn({ err }, 'cache degraded; serving from postgres');

export async function resolveCategoryId(deps: AppDeps, slug: string): Promise<number | null> {
  const { value } = await readThrough<number | null>(deps.redis, keys.categorySlug(slug), async () => {
    const [row] = await deps.db.select({ id: categories.id }).from(categories).where(eq(categories.slug, slug));
    return { value: row?.id ?? null, ttlSeconds: row ? SLUG_TTL_SECONDS : 5 };
  }, { onError: onError(deps) });
  return value;
}

export async function getCachedProduct(deps: AppDeps, id: number): Promise<ProductRecord | null> {
  const redis = deps.redis;
  const { value } = await readThrough<ProductEntry | null>(redis, keys.product(id), async () => {
    const now = deps.now();
    const [productVersion] = redis ? await getVersions(redis, [keys.productVersion(id)]) : [0];
    const record = await fetchProductById(deps.db, id, now);
    if (!record) return { value: null, ttlSeconds: 5 };
    const [categoryVersion] = redis ? await getVersions(redis, [keys.categoryVersion(record.category.id)]) : [0];
    const boundary = await nextPromotionBoundary(deps.db, { productId: id, categoryId: record.category.id }, now);
    return {
      value: { productVersion: productVersion ?? 0, categoryVersion: categoryVersion ?? 0, categoryId: record.category.id, record },
      ttlSeconds: ttlSeconds(now, boundary),
    };
  }, {
    onError: onError(deps),
    isFresh: async (entry) => {
      if (!entry || !redis) return true;
      const [pv, cv] = await getVersions(redis, [keys.productVersion(id), keys.categoryVersion(entry.categoryId)]);
      return entry.productVersion === pv && entry.categoryVersion === cv;
    },
  });
  return value?.record ?? null;
}

export async function getCachedProductPage(
  deps: AppDeps,
  opts: { categoryId: number | null; sort: SortDir; page: number; pageSize: number },
): Promise<{ items: ProductRecord[]; total: number }> {
  const redis = deps.redis;
  const versionKey = opts.categoryId === null ? keys.allVersion() : keys.categoryVersion(opts.categoryId);
  const { value } = await readThrough<PageEntry>(redis, keys.list(opts.categoryId, opts.sort, opts.page, opts.pageSize), async () => {
    const now = deps.now();
    const [version] = redis ? await getVersions(redis, [versionKey]) : [0];
    const page = await fetchProductPage(deps.db, {
      categoryId: opts.categoryId, sort: opts.sort, limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize, now,
    });
    const boundary = await nextPromotionBoundary(deps.db, { categoryId: opts.categoryId }, now);
    return { value: { version: version ?? 0, ...page }, ttlSeconds: ttlSeconds(now, boundary) };
  }, {
    onError: onError(deps),
    isFresh: async (entry) => {
      if (!redis) return true;
      const [v] = await getVersions(redis, [versionKey]);
      return entry.version === v;
    },
  });
  return { items: value.items, total: value.total };
}
```

- [ ] **Step 4: Service and routes**

`apps/api/src/products/service.ts` (create and stock methods are added in Task 9):
```ts
import type { ProductRecord } from '@modaco/core';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { getCachedProduct, getCachedProductPage, resolveCategoryId } from './cached-reads';
import type { ListQuery } from './schemas';
import { loadStocks } from './stock';

export interface ProductItem extends ProductRecord { stock: number }

export class ProductService {
  constructor(private readonly deps: AppDeps) {}

  async getProduct(id: number): Promise<ProductItem | null> {
    const record = await getCachedProduct(this.deps, id);
    if (!record) return null;
    const stocks = await loadStocks(this.deps, [id]);
    return { ...record, stock: stocks.get(id) ?? 0 };
  }

  async listProducts(q: ListQuery): Promise<{ items: ProductItem[]; pagination: { page: number; pageSize: number; total: number } }> {
    let categoryId: number | null = null;
    if (q.category) {
      categoryId = await resolveCategoryId(this.deps, q.category);
      if (categoryId === null) throw notFound(`category '${q.category}' not found`);
    }
    const sort = q.sort === '-effective_price' ? 'desc' : 'asc';
    const page = await getCachedProductPage(this.deps, { categoryId, sort, page: q.page, pageSize: q.pageSize });
    const stocks = await loadStocks(this.deps, page.items.map((i) => i.id));
    return {
      items: page.items.map((i) => ({ ...i, stock: stocks.get(i.id) ?? 0 })),
      pagination: { page: q.page, pageSize: q.pageSize, total: page.total },
    };
  }
}
```

`apps/api/src/products/routes.ts`:
```ts
import { Router } from 'express';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { idParam, listQuery, type ListQuery } from './schemas';
import { ProductService } from './service';

export function productRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new ProductService(deps);

  r.get('/products', validate({ query: listQuery }), async (_req, res) => {
    const { query } = input<unknown, ListQuery>(res);
    res.json(await service.listProducts(query));
  });

  r.get('/products/:id', validate({ params: idParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: number }>(res);
    const item = await service.getProduct(params.id);
    if (!item) throw notFound(`product ${params.id} not found`);
    res.json(item);
  });

  return r;
}
```

Modify `apps/api/src/app.ts`: import `productRoutes` and add `app.use(productRoutes(deps));` after the health routes and before the 404 handler.

- [ ] **Step 5: Run tests**

Run: `pnpm --filter @modaco/api test test/products.test.ts`
Expected: PASS, 9 tests.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(api): product list and detail with version-validated redis cache and live stock

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Product create and stock adjustment

**Files:**
- Modify: `apps/api/src/products/service.ts`, `apps/api/src/products/routes.ts`, `apps/api/test/products.test.ts`

**Interfaces:**
- Produces: `ProductService.createProduct(body: CreateProductBody): Promise<ProductItem>` (throws `notFound` for unknown category, `conflict` on duplicate sku), `ProductService.adjustStock(id: number, body: StockBody): Promise<{ id: number; stock: number } | null>` (throws `unprocessable` if the result would be negative).

- [ ] **Step 1: Add failing tests to `apps/api/test/products.test.ts`**

```ts
describe('POST /products', () => {
  it('creates a product that immediately inherits an active category promotion', async () => {
    await activePromo();
    await request(ctx.app).get('/products?category=accessories'); // warm the list cache
    const res = await request(ctx.app).post('/products').send({ sku: 'RING', name: 'Ring', categoryId: acc.id, basePrice: '30.00', stock: 2 });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ sku: 'RING', effectivePrice: '15.00', stock: 2, activePromotion: { name: 'Sale' } });
    const list = await request(ctx.app).get('/products?category=accessories');
    expect(list.body.pagination.total).toBe(3);
    expect(list.body.items.find((i: { sku: string }) => i.sku === 'RING').effectivePrice).toBe('15.00');
  });
  it('rejects a duplicate sku, unknown category, and bad body', async () => {
    expect((await request(ctx.app).post('/products').send({ sku: 'BELT', name: 'x', categoryId: acc.id, basePrice: '1.00' })).status).toBe(409);
    expect((await request(ctx.app).post('/products').send({ sku: 'NEW', name: 'x', categoryId: 999, basePrice: '1.00' })).status).toBe(404);
    expect((await request(ctx.app).post('/products').send({ sku: 'NEW', name: 'x', categoryId: acc.id, basePrice: '1.999' })).status).toBe(400);
  });
});

describe('PATCH /products/:id/stock', () => {
  it('applies a delta and an absolute value, visible immediately', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    let res = await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ delta: -2 });
    expect(res.body).toEqual({ id: belt.id, stock: 1 });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(1);
    res = await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ stock: 10 });
    expect(res.body).toEqual({ id: belt.id, stock: 10 });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(10);
  });
  it('refuses to go negative and 404s on unknown ids', async () => {
    expect((await request(ctx.app).patch(`/products/${hat.id}/stock`).send({ delta: -1 })).status).toBe(422);
    expect((await request(ctx.app).patch('/products/999999/stock').send({ delta: 1 })).status).toBe(404);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/api test test/products.test.ts`
Expected: the four new tests FAIL with 404.

- [ ] **Step 3: Extend the service**

Add to `apps/api/src/products/service.ts` (imports: `eq`, `sql` from `drizzle-orm`; `categories`, `products`, `bumpCategory`, `bumpProduct` from `@modaco/core`; `conflict`, `unprocessable`, `pgErrorCode` from `../errors`; `setStock` from `./stock`; `CreateProductBody`, `StockBody` from `./schemas`):
```ts
  async createProduct(body: CreateProductBody): Promise<ProductItem> {
    const [cat] = await this.deps.db.select({ id: categories.id }).from(categories).where(eq(categories.id, body.categoryId));
    if (!cat) throw notFound(`category ${body.categoryId} not found`);
    let id: number;
    try {
      const [row] = await this.deps.db.insert(products).values({
        sku: body.sku, name: body.name, categoryId: body.categoryId, basePrice: body.basePrice, stock: body.stock,
      }).returning({ id: products.id });
      id = row!.id;
    } catch (err) {
      if (pgErrorCode(err) === '23505') throw conflict(`sku '${body.sku}' already exists`);
      throw err;
    }
    if (this.deps.redis) {
      await bumpCategory(this.deps.redis, body.categoryId, (msg, err) => this.deps.logger.error({ err }, msg));
    }
    await setStock(this.deps, id, body.stock);
    return (await this.getProduct(id))!;
  }

  async adjustStock(id: number, body: StockBody): Promise<{ id: number; stock: number } | null> {
    const [exists] = await this.deps.db.select({ stock: products.stock }).from(products).where(eq(products.id, id));
    if (!exists) return null;
    const set = 'delta' in body
      ? { stock: sql`${products.stock} + ${body.delta}`, updatedAt: sql`now()` }
      : { stock: body.stock, updatedAt: sql`now()` };
    let row: { id: number; stock: number } | undefined;
    try {
      [row] = await this.deps.db.update(products).set(set).where(eq(products.id, id)).returning({ id: products.id, stock: products.stock });
    } catch (err) {
      if (pgErrorCode(err) === '23514') throw unprocessable('stock cannot go below zero');
      throw err;
    }
    await setStock(this.deps, id, row!.stock);
    return row!;
  }
```

- [ ] **Step 4: Extend the routes**

Add to `apps/api/src/products/routes.ts` (import `createProductBody`, `stockBody`, `CreateProductBody`, `StockBody`):
```ts
  r.post('/products', validate({ body: createProductBody }), async (_req, res) => {
    const { body } = input<CreateProductBody>(res);
    res.status(201).json(await service.createProduct(body));
  });

  r.patch('/products/:id/stock', validate({ params: idParam, body: stockBody }), async (_req, res) => {
    const { params, body } = input<StockBody, unknown, { id: number }>(res);
    const result = await service.adjustStock(params.id, body);
    if (!result) throw notFound(`product ${params.id} not found`);
    res.json(result);
  });
```

- [ ] **Step 5: Run tests**

Run: `pnpm --filter @modaco/api test test/products.test.ts`
Expected: PASS, 13 tests.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(api): product creation with category version bump and live stock adjustment

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Promotion endpoints

**Files:**
- Create: `apps/api/src/promotions/schemas.ts`, `apps/api/src/promotions/service.ts`, `apps/api/src/promotions/routes.ts`, `apps/api/test/promotions.test.ts`
- Modify: `apps/api/src/app.ts`

**Interfaces:**
- Produces:
  - `interface PromotionView { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string; startsAt: string; endsAt: string; target: { productId: number } | { categoryId: number }; cancelledAt: string | null; createdAt: string }`
  - `class PromotionService { create(body): Promise<PromotionView>; cancel(id): Promise<PromotionView | null>; assign(id, target): Promise<PromotionView | null>; get(id): Promise<PromotionView | null> }`
  - `promotionRoutes(deps): Router`

- [ ] **Step 1: Write the failing promotion tests**

`apps/api/test/promotions.test.ts`:
```ts
import { keys } from '@modaco/core';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
let acc: { id: number; slug: string };
let shoes: { id: number; slug: string };
let belt: { id: number };
let boot: { id: number };

const iso = (offsetMs: number) => new Date(Date.now() + offsetMs).toISOString();
const body = (over: Record<string, unknown> = {}) => ({
  name: 'Sale', discountType: 'percentage', value: '50', startsAt: iso(-60_000), endsAt: iso(3_600_000), target: { categoryId: acc.id }, ...over,
});

beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  acc = await ctx.insertCategory('Accessories');
  shoes = await ctx.insertCategory('Shoes');
  belt = await ctx.insertProduct({ categoryId: acc.id, sku: 'BELT', basePrice: '20.00' });
  boot = await ctx.insertProduct({ categoryId: shoes.id, sku: 'BOOT', basePrice: '100.00' });
});

describe('POST /promotions', () => {
  it('creates a category promotion and prices flip instantly for every product in it', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    await request(ctx.app).get('/products?category=accessories');
    const res = await request(ctx.app).post('/promotions').send(body());
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ name: 'Sale', discountType: 'percentage', value: '50.00', target: { categoryId: acc.id }, cancelledAt: null });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    expect((await request(ctx.app).get('/products?category=accessories')).body.items[0].effectivePrice).toBe('10.00');
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('100.00');
    expect(await ctx.redis.get(keys.categoryVersion(acc.id))).toBe('1');
    expect(await ctx.redis.get(keys.allVersion())).toBe('1');
  });

  it('creates a product promotion and bumps only the product version', async () => {
    const res = await request(ctx.app).post('/promotions').send(body({ target: { productId: belt.id }, discountType: 'fixed', value: '5.00' }));
    expect(res.status).toBe(201);
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('15.00');
    expect(await ctx.redis.get(keys.productVersion(belt.id))).toBe('1');
    expect(await ctx.redis.get(keys.categoryVersion(acc.id))).toBeNull();
  });

  it('most recent promotion wins', async () => {
    await request(ctx.app).post('/promotions').send(body({ target: { productId: belt.id }, value: '10' }));
    await request(ctx.app).post('/promotions').send(body({ value: '50' }));
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });

  it('validates semantics', async () => {
    expect((await request(ctx.app).post('/promotions').send(body({ startsAt: iso(10), endsAt: iso(0) }))).status).toBe(422);
    expect((await request(ctx.app).post('/promotions').send(body({ value: '150' }))).status).toBe(422);
    expect((await request(ctx.app).post('/promotions').send(body({ target: { categoryId: 9999 } }))).status).toBe(404);
    expect((await request(ctx.app).post('/promotions').send(body({ target: {} }))).status).toBe(400);
    expect((await request(ctx.app).post('/promotions').send(body({ value: 'abc' }))).status).toBe(400);
  });
});

describe('POST /promotions/:id/cancel', () => {
  it('cancels idempotently and restores prices', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    const first = await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    expect(first.status).toBe(200);
    expect(first.body.cancelledAt).not.toBeNull();
    const second = await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    expect(second.body.cancelledAt).toBe(first.body.cancelledAt);
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    expect((await request(ctx.app).post('/promotions/00000000-0000-0000-0000-000000000000/cancel')).status).toBe(404);
  });
});

describe('PUT /promotions/:id/target', () => {
  it('moves a promotion and bumps both old and new targets', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    const res = await request(ctx.app).put(`/promotions/${promo.id}/target`).send({ categoryId: shoes.id });
    expect(res.status).toBe(200);
    expect(res.body.target).toEqual({ categoryId: shoes.id });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('50.00');
    const toProduct = await request(ctx.app).put(`/promotions/${promo.id}/target`).send({ productId: belt.id });
    expect(toProduct.body.target).toEqual({ productId: belt.id });
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('100.00');
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });
});

describe('GET /promotions/:id', () => {
  it('returns the promotion or 404', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/promotions/${promo.id}`)).body.id).toBe(promo.id);
    expect((await request(ctx.app).get('/promotions/not-a-uuid')).status).toBe(400);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/api test test/promotions.test.ts`
Expected: FAIL with 404s.

- [ ] **Step 3: Implement schemas and service**

`apps/api/src/promotions/schemas.ts`:
```ts
import { z } from 'zod';

export const promotionIdParam = z.object({ id: z.string().uuid() });

export const targetSchema = z.union([
  z.object({ productId: z.number().int().positive() }).strict(),
  z.object({ categoryId: z.number().int().positive() }).strict(),
]);
export type Target = z.infer<typeof targetSchema>;

export const createPromotionBody = z.object({
  name: z.string().trim().min(1).max(255),
  discountType: z.enum(['percentage', 'fixed']),
  value: z.string().regex(/^\d+(\.\d{1,2})?$/, 'value must be a decimal with up to 2 places'),
  startsAt: z.string().datetime({ offset: true }),
  endsAt: z.string().datetime({ offset: true }),
  target: targetSchema,
});
export type CreatePromotionBody = z.infer<typeof createPromotionBody>;
```

`apps/api/src/promotions/service.ts`:
```ts
import { eq } from 'drizzle-orm';
import { bumpCategory, bumpProduct, categories, products, promotions, type Redis } from '@modaco/core';
import type { AppDeps } from '../deps';
import { notFound, unprocessable } from '../errors';
import type { CreatePromotionBody, Target } from './schemas';

export interface PromotionView {
  id: string; name: string; discountType: 'percentage' | 'fixed'; value: string;
  startsAt: string; endsAt: string; target: Target; cancelledAt: string | null; createdAt: string;
}

type Row = typeof promotions.$inferSelect;

function toView(r: Row): PromotionView {
  return {
    id: r.id, name: r.name, discountType: r.discountType, value: r.value,
    startsAt: r.startsAt.toISOString(), endsAt: r.endsAt.toISOString(),
    target: r.scope === 'product' ? { productId: r.productId! } : { categoryId: r.categoryId! },
    cancelledAt: r.cancelledAt ? r.cancelledAt.toISOString() : null,
    createdAt: r.createdAt.toISOString(),
  };
}

export class PromotionService {
  constructor(private readonly deps: AppDeps) {}

  private async assertTarget(target: Target): Promise<void> {
    if ('productId' in target) {
      const [p] = await this.deps.db.select({ id: products.id }).from(products).where(eq(products.id, target.productId));
      if (!p) throw notFound(`product ${target.productId} not found`);
    } else {
      const [c] = await this.deps.db.select({ id: categories.id }).from(categories).where(eq(categories.id, target.categoryId));
      if (!c) throw notFound(`category ${target.categoryId} not found`);
    }
  }

  private async bump(target: Target): Promise<void> {
    const redis: Redis | null = this.deps.redis;
    if (!redis) return;
    const log = (msg: string, err: unknown) => this.deps.logger.error({ err }, msg);
    if ('productId' in target) await bumpProduct(redis, target.productId, log);
    else await bumpCategory(redis, target.categoryId, log);
  }

  async create(body: CreatePromotionBody): Promise<PromotionView> {
    const startsAt = new Date(body.startsAt);
    const endsAt = new Date(body.endsAt);
    if (endsAt <= startsAt) throw unprocessable('endsAt must be after startsAt');
    if (body.discountType === 'percentage' && Number(body.value) > 100) throw unprocessable('percentage value cannot exceed 100');
    await this.assertTarget(body.target);
    const [row] = await this.deps.db.insert(promotions).values({
      name: body.name, discountType: body.discountType, value: body.value, startsAt, endsAt,
      scope: 'productId' in body.target ? 'product' : 'category',
      productId: 'productId' in body.target ? body.target.productId : null,
      categoryId: 'categoryId' in body.target ? body.target.categoryId : null,
    }).returning();
    await this.bump(body.target);
    return toView(row!);
  }

  async get(id: string): Promise<PromotionView | null> {
    const [row] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    return row ? toView(row) : null;
  }

  async cancel(id: string): Promise<PromotionView | null> {
    const [existing] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    if (!existing) return null;
    if (existing.cancelledAt) return toView(existing);
    const [row] = await this.deps.db.update(promotions).set({ cancelledAt: this.deps.now() }).where(eq(promotions.id, id)).returning();
    await this.bump(toView(row!).target);
    return toView(row!);
  }

  async assign(id: string, target: Target): Promise<PromotionView | null> {
    const [existing] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    if (!existing) return null;
    await this.assertTarget(target);
    const [row] = await this.deps.db.update(promotions).set({
      scope: 'productId' in target ? 'product' : 'category',
      productId: 'productId' in target ? target.productId : null,
      categoryId: 'categoryId' in target ? target.categoryId : null,
    }).where(eq(promotions.id, id)).returning();
    await this.bump(toView(existing).target);
    await this.bump(target);
    return toView(row!);
  }
}
```

`apps/api/src/promotions/routes.ts`:
```ts
import { Router } from 'express';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { createPromotionBody, promotionIdParam, targetSchema, type CreatePromotionBody, type Target } from './schemas';
import { PromotionService } from './service';

export function promotionRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new PromotionService(deps);

  r.post('/promotions', validate({ body: createPromotionBody }), async (_req, res) => {
    const { body } = input<CreatePromotionBody>(res);
    res.status(201).json(await service.create(body));
  });

  r.get('/promotions/:id', validate({ params: promotionIdParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const view = await service.get(params.id);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  r.post('/promotions/:id/cancel', validate({ params: promotionIdParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const view = await service.cancel(params.id);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  r.put('/promotions/:id/target', validate({ params: promotionIdParam, body: targetSchema }), async (_req, res) => {
    const { params, body } = input<Target, unknown, { id: string }>(res);
    const view = await service.assign(params.id, body);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  return r;
}
```

Modify `apps/api/src/app.ts`: add `app.use(promotionRoutes(deps));` after the product routes.

- [ ] **Step 4: Run tests**

Run: `pnpm --filter @modaco/api test`
Expected: PASS, all API tests.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(api): promotion create, cancel, assign with single version bump invalidation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Ingestion job endpoints

**Files:**
- Create: `apps/api/src/ingestion/service.ts`, `apps/api/src/ingestion/routes.ts`, `apps/api/test/ingestion-routes.test.ts`
- Modify: `apps/api/src/app.ts`

**Interfaces:**
- Produces:
  - `class IngestionService { createJob(filename?: string): Promise<{ jobId: string; uploadUrl: string; key: string; expiresInSeconds: number }>; getJob(id): Promise<JobView | null>; listRejections(id, page, pageSize): Promise<{ items: RejectionView[]; pagination } | null> }`
  - `JobView = { id, status, s3Key, totalChunks, completedChunks, failedChunks, rowsProcessed, rowsRejected, error, createdAt, updatedAt }`
  - `ingestionRoutes(deps): Router`

- [ ] **Step 1: Write the failing tests**

`apps/api/test/ingestion-routes.test.ts`:
```ts
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { ingestionJobs, ingestionRejections } from '@modaco/core';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(() => ctx.truncateAll());

describe('POST /ingestion/jobs', () => {
  it('creates a pending job and a presigned PUT url that works against localstack', async () => {
    const res = await request(ctx.app).post('/ingestion/jobs').send({ filename: 'vendor.csv' });
    expect(res.status).toBe(201);
    expect(res.body.key).toBe(`uploads/${res.body.jobId}/vendor.csv`);
    expect(res.body.uploadUrl).toContain('X-Amz-Signature');
    const put = await fetch(res.body.uploadUrl, { method: 'PUT', body: 'sku,name,category,vendor_price,stock\n' });
    expect(put.status).toBe(200);
    const head = await ctx.deps.s3.send(new HeadObjectCommand({ Bucket: ctx.deps.config.s3Bucket, Key: res.body.key }));
    expect(head.ContentLength).toBe(37);
    const job = await request(ctx.app).get(`/ingestion/jobs/${res.body.jobId}`);
    expect(job.body).toMatchObject({ id: res.body.jobId, status: 'pending', totalChunks: 0 });
  });
  it('defaults the filename and rejects bad names', async () => {
    const res = await request(ctx.app).post('/ingestion/jobs').send({});
    expect(res.body.key).toMatch(/^uploads\/[0-9a-f-]{36}\/vendor\.csv$/);
    expect((await request(ctx.app).post('/ingestion/jobs').send({ filename: '../x.csv' })).status).toBe(400);
  });
});

describe('GET /ingestion/jobs/:id/rejections', () => {
  it('pages through rejections', async () => {
    const [job] = await ctx.db.insert(ingestionJobs).values({ s3Key: 'uploads/x/y.csv' }).returning();
    await ctx.db.insert(ingestionRejections).values([1, 2, 3].map((n) => ({ jobId: job!.id, chunkIndex: 0, lineNumber: n, rawLine: `bad,${n}`, reason: 'validation' })));
    const res = await request(ctx.app).get(`/ingestion/jobs/${job!.id}/rejections?pageSize=2&page=2`);
    expect(res.body.items).toHaveLength(1);
    expect(res.body.items[0]).toMatchObject({ lineNumber: 3, rawLine: 'bad,3', reason: 'validation' });
    expect(res.body.pagination).toEqual({ page: 2, pageSize: 2, total: 3 });
    expect((await request(ctx.app).get('/ingestion/jobs/00000000-0000-0000-0000-000000000000/rejections')).status).toBe(404);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/api test test/ingestion-routes.test.ts`
Expected: FAIL with 404s.

- [ ] **Step 3: Implement**

`apps/api/src/ingestion/service.ts`:
```ts
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { asc, count, eq } from 'drizzle-orm';
import { ingestionJobs, ingestionRejections } from '@modaco/core';
import type { AppDeps } from '../deps';

export const PRESIGN_TTL_SECONDS = 900;

type JobRow = typeof ingestionJobs.$inferSelect;
export interface JobView {
  id: string; status: JobRow['status']; s3Key: string; totalChunks: number; completedChunks: number; failedChunks: number;
  rowsProcessed: number; rowsRejected: number; error: string | null; createdAt: string; updatedAt: string;
}
export interface RejectionView { chunkIndex: number; lineNumber: number; rawLine: string; reason: string }

const toJobView = (r: JobRow): JobView => ({
  id: r.id, status: r.status, s3Key: r.s3Key, totalChunks: r.totalChunks, completedChunks: r.completedChunks,
  failedChunks: r.failedChunks, rowsProcessed: r.rowsProcessed, rowsRejected: r.rowsRejected, error: r.error,
  createdAt: r.createdAt.toISOString(), updatedAt: r.updatedAt.toISOString(),
});

export class IngestionService {
  constructor(private readonly deps: AppDeps) {}

  async createJob(filename = 'vendor.csv'): Promise<{ jobId: string; uploadUrl: string; key: string; expiresInSeconds: number }> {
    const [job] = await this.deps.db.insert(ingestionJobs).values({ s3Key: 'pending' }).returning({ id: ingestionJobs.id });
    const key = `uploads/${job!.id}/${filename}`;
    await this.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, job!.id));
    const uploadUrl = await getSignedUrl(
      this.deps.presigner,
      new PutObjectCommand({ Bucket: this.deps.config.s3Bucket, Key: key }), // no ContentType: a signed header would force every uploader to match it exactly
      { expiresIn: PRESIGN_TTL_SECONDS },
    );
    return { jobId: job!.id, uploadUrl, key, expiresInSeconds: PRESIGN_TTL_SECONDS };
  }

  async getJob(id: string): Promise<JobView | null> {
    const [row] = await this.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, id));
    return row ? toJobView(row) : null;
  }

  async listRejections(id: string, page: number, pageSize: number) {
    if (!(await this.getJob(id))) return null;
    const [items, [{ total }]] = await Promise.all([
      this.deps.db.select({
        chunkIndex: ingestionRejections.chunkIndex, lineNumber: ingestionRejections.lineNumber,
        rawLine: ingestionRejections.rawLine, reason: ingestionRejections.reason,
      }).from(ingestionRejections).where(eq(ingestionRejections.jobId, id))
        .orderBy(asc(ingestionRejections.chunkIndex), asc(ingestionRejections.lineNumber))
        .limit(pageSize).offset((page - 1) * pageSize),
      this.deps.db.select({ total: count() }).from(ingestionRejections).where(eq(ingestionRejections.jobId, id)) as Promise<[{ total: number }]>,
    ]);
    return { items: items as RejectionView[], pagination: { page, pageSize, total: Number(total) } };
  }
}
```

`apps/api/src/ingestion/routes.ts`:
```ts
import { Router } from 'express';
import { z } from 'zod';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { IngestionService } from './service';

const createBody = z.object({ filename: z.string().regex(/^[A-Za-z0-9._-]{1,128}$/).optional() });
const jobParam = z.object({ id: z.string().uuid() });
const pageQuery = z.object({ page: z.coerce.number().int().min(1).default(1), pageSize: z.coerce.number().int().min(1).max(100).default(20) });

export function ingestionRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new IngestionService(deps);

  r.post('/ingestion/jobs', validate({ body: createBody }), async (_req, res) => {
    const { body } = input<{ filename?: string }>(res);
    res.status(201).json(await service.createJob(body.filename));
  });

  r.get('/ingestion/jobs/:id', validate({ params: jobParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const job = await service.getJob(params.id);
    if (!job) throw notFound(`job ${params.id} not found`);
    res.json(job);
  });

  r.get('/ingestion/jobs/:id/rejections', validate({ params: jobParam, query: pageQuery }), async (_req, res) => {
    const { params, query } = input<unknown, { page: number; pageSize: number }, { id: string }>(res);
    const result = await service.listRejections(params.id, query.page, query.pageSize);
    if (!result) throw notFound(`job ${params.id} not found`);
    res.json(result);
  });

  return r;
}
```

Modify `apps/api/src/app.ts`: add `app.use(ingestionRoutes(deps));` after the promotion routes.

- [ ] **Step 4: Run tests**

Run: `pnpm --filter @modaco/api test`
Expected: PASS. If the presigned PUT returns 403 from LocalStack, confirm `S3_PUBLIC_ENDPOINT` and `AWS_ENDPOINT_URL` are both `http://localhost:4566` when running on the host; the signature covers the host header.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(api): ingestion job creation with presigned upload, status, rejections

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: Ingest package, splitter handler

**Files:**
- Create: `apps/ingest/package.json`, `apps/ingest/tsconfig.json`, `apps/ingest/vitest.config.ts`, `apps/ingest/src/config.ts`, `apps/ingest/src/aws.ts`, `apps/ingest/src/deps.ts`, `apps/ingest/src/splitter.ts`, `apps/ingest/test/helpers.ts`, `apps/ingest/test/splitter.test.ts`

**Interfaces:**
- Produces:
  - `loadIngestConfig(env?): IngestConfig` with `databaseUrl, redisUrl, awsRegion, awsEndpointUrl (string | undefined), s3Bucket, s3EventsQueueUrl, chunkQueueUrl, dlqUrl, chunkSizeBytes, upsertBatchSize, lambdaTimeoutMs, lambdaMemoryMb, logLevel`
  - `createS3(config): S3Client`, `createSqs(config): SQSClient`
  - `interface IngestDeps { db: Db; redis: Redis; s3: S3Client; sqs: SQSClient; config: IngestConfig; logger: Logger }`
  - `createIngestDeps(config): Promise<IngestDeps & { close(): Promise<void> }>`, `getDeps(): Promise<IngestDeps>` (process-wide singleton for warm Lambda invocations)
  - `jobIdFromKey(key: string): string | null`
  - `interface ChunkMessage { jobId: string; chunkIndex: number; byteStart: number; byteEnd: number }`, `chunkMessageSchema` (Zod)
  - `splitUpload(deps, input: { key: string }): Promise<{ jobId: string; totalChunks: number; contentLength: number } | null>` (null when no job matches the key)
  - Test helpers: `setupIngestTest(): Promise<IngestTestContext>` with `deps, truncateAll(), drainQueue(url), receiveAll(url, expected, timeoutMs), putObject(key, body), createJob(key): Promise<string>, close()`

- [ ] **Step 1: Package files**

`apps/ingest/package.json`:
```json
{
  "name": "@modaco/ingest",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "scripts": {
    "runner": "tsx src/runner.ts",
    "build": "node build.mjs",
    "test": "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-s3": "^3.700.0",
    "@aws-sdk/client-sqs": "^3.700.0",
    "@modaco/core": "workspace:*",
    "drizzle-orm": "^0.44.2",
    "ioredis": "^5.4.1",
    "pino": "^9.5.0",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@types/aws-lambda": "^8.10.145",
    "esbuild": "^0.24.0",
    "tsx": "^4.19.1",
    "typescript": "^5.6.3",
    "vitest": "^3.0.5"
  }
}
```

`apps/ingest/tsconfig.json`:
```json
{ "extends": "../../tsconfig.base.json", "include": ["src", "test"] }
```

`apps/ingest/vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';
export default defineConfig({ test: { include: ['test/**/*.test.ts'], testTimeout: 120000, hookTimeout: 60000, fileParallelism: false } });
```

- [ ] **Step 2: Config, AWS clients, deps**

`apps/ingest/src/config.ts`:
```ts
import { z } from 'zod';

const schema = z.object({
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  AWS_ENDPOINT_URL: z.string().optional(),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  S3_EVENTS_QUEUE_URL: z.string().default('http://localhost:4566/000000000000/modaco-s3-events'),
  CHUNK_QUEUE_URL: z.string().default('http://localhost:4566/000000000000/modaco-ingest-chunks'),
  DLQ_URL: z.string().default('http://localhost:4566/000000000000/modaco-ingest-dlq'),
  CHUNK_SIZE_BYTES: z.coerce.number().int().positive().default(4_194_304),
  UPSERT_BATCH_SIZE: z.coerce.number().int().positive().default(1000),
  LAMBDA_TIMEOUT_MS: z.coerce.number().int().positive().default(60_000),
  LAMBDA_MEMORY_MB: z.coerce.number().int().positive().default(256),
  LOG_LEVEL: z.string().default('info'),
});

export interface IngestConfig {
  databaseUrl: string; redisUrl: string; awsRegion: string; awsEndpointUrl: string | undefined; s3Bucket: string;
  s3EventsQueueUrl: string; chunkQueueUrl: string; dlqUrl: string; chunkSizeBytes: number; upsertBatchSize: number;
  lambdaTimeoutMs: number; lambdaMemoryMb: number; logLevel: string;
}

export function loadIngestConfig(env: NodeJS.ProcessEnv = process.env): IngestConfig {
  const e = schema.parse(env);
  return {
    databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION, awsEndpointUrl: e.AWS_ENDPOINT_URL,
    s3Bucket: e.S3_BUCKET, s3EventsQueueUrl: e.S3_EVENTS_QUEUE_URL, chunkQueueUrl: e.CHUNK_QUEUE_URL, dlqUrl: e.DLQ_URL,
    chunkSizeBytes: e.CHUNK_SIZE_BYTES, upsertBatchSize: e.UPSERT_BATCH_SIZE, lambdaTimeoutMs: e.LAMBDA_TIMEOUT_MS,
    lambdaMemoryMb: e.LAMBDA_MEMORY_MB, logLevel: e.LOG_LEVEL,
  };
}
```

`apps/ingest/src/aws.ts`:
```ts
import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { IngestConfig } from './config';

/** With an endpoint (LocalStack) use path-style and static test credentials; without one, the default AWS provider chain. */
function base(config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) {
  return config.awsEndpointUrl
    ? {
        region: config.awsRegion, endpoint: config.awsEndpointUrl,
        credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
      }
    : { region: config.awsRegion };
}

export const createS3 = (config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) =>
  new S3Client({ ...base(config), forcePathStyle: Boolean(config.awsEndpointUrl) });
export const createSqs = (config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) => new SQSClient(base(config));
```

`apps/ingest/src/deps.ts`:
```ts
import type { S3Client } from '@aws-sdk/client-s3';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { createDb, createRedis, type Db, type Redis } from '@modaco/core';
import pino, { type Logger } from 'pino';
import { createS3, createSqs } from './aws';
import { loadIngestConfig, type IngestConfig } from './config';

export interface IngestDeps {
  db: Db;
  redis: Redis;
  s3: S3Client;
  sqs: SQSClient;
  config: IngestConfig;
  logger: Logger;
}

export async function createIngestDeps(config: IngestConfig): Promise<IngestDeps & { close(): Promise<void> }> {
  const logger = pino({ level: config.logLevel });
  const { db, close: closeDb } = createDb(config.databaseUrl, { max: 2 });
  const redis = createRedis(config.redisUrl);
  redis.on('error', (err) => logger.warn({ err }, 'redis error'));
  await redis.connect().catch((err) => logger.warn({ err }, 'redis connect failed; version bumps will be retried'));
  return {
    db, redis, s3: createS3(config), sqs: createSqs(config), config, logger,
    close: async () => { await closeDb(); redis.disconnect(); },
  };
}

let singleton: Promise<IngestDeps> | undefined;
/** Reused across warm invocations of the same Lambda container. */
export function getDeps(): Promise<IngestDeps> {
  singleton ??= createIngestDeps(loadIngestConfig());
  return singleton;
}
```

- [ ] **Step 3: Test helpers**

`apps/ingest/test/helpers.ts`:
```ts
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { DeleteMessageCommand, ReceiveMessageCommand, type Message } from '@aws-sdk/client-sqs';
import { sql } from 'drizzle-orm';
import { ingestionJobs, runMigrations } from '@modaco/core';
import { loadIngestConfig } from '../src/config';
import { createIngestDeps, type IngestDeps } from '../src/deps';

export interface IngestTestContext {
  deps: IngestDeps;
  truncateAll(): Promise<void>;
  drainQueue(url: string): Promise<void>;
  receiveAll(url: string, expected: number, timeoutMs?: number): Promise<Message[]>;
  putObject(key: string, body: string | Buffer): Promise<void>;
  createJob(key: string): Promise<string>;
  close(): Promise<void>;
}

export async function setupIngestTest(env: Record<string, string> = {}): Promise<IngestTestContext> {
  const config = loadIngestConfig({ ...process.env, AWS_ENDPOINT_URL: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566', LOG_LEVEL: 'silent', ...env });
  const real = await createIngestDeps(config);
  await runMigrations(real.db);
  const receive = (url: string) => real.sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
  const del = (url: string, m: Message) => real.sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle! }));
  return {
    deps: real,
    truncateAll: async () => {
      await real.db.execute(sql`truncate ingestion_rejections, ingestion_chunks, ingestion_jobs, promotions, products, categories restart identity cascade`);
      await real.redis.flushdb();
    },
    drainQueue: async (url) => {
      for (;;) {
        const res = await receive(url);
        if (!res.Messages?.length) return;
        await Promise.all(res.Messages.map((m) => del(url, m)));
      }
    },
    receiveAll: async (url, expected, timeoutMs = 20_000) => {
      const out: Message[] = [];
      const deadline = Date.now() + timeoutMs;
      while (out.length < expected && Date.now() < deadline) {
        const res = await receive(url);
        for (const m of res.Messages ?? []) { out.push(m); await del(url, m); }
      }
      return out;
    },
    putObject: async (key, body) => { await real.s3.send(new PutObjectCommand({ Bucket: config.s3Bucket, Key: key, Body: body })); },
    createJob: async (key) => {
      const [job] = await real.db.insert(ingestionJobs).values({ s3Key: key }).returning({ id: ingestionJobs.id });
      return job!.id;
    },
    close: () => real.close(),
  };
}
```

- [ ] **Step 4: Write the failing splitter test**

`apps/ingest/test/splitter.test.ts`:
```ts
import { eq } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { jobIdFromKey, splitUpload } from '../src/splitter';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ CHUNK_SIZE_BYTES: '4' }); });
afterAll(() => ctx.close());
beforeEach(async () => { await ctx.truncateAll(); await ctx.drainQueue(ctx.deps.config.chunkQueueUrl); });

describe('jobIdFromKey', () => {
  it('extracts the job id from an upload key', () => {
    expect(jobIdFromKey('uploads/0b6a1a4e-2c1b-4c2a-9f7f-8d1e7a5b3c21/vendor.csv')).toBe('0b6a1a4e-2c1b-4c2a-9f7f-8d1e7a5b3c21');
    expect(jobIdFromKey('other/x.csv')).toBeNull();
  });
});

describe('splitUpload', () => {
  it('enqueues one byte-range message per chunk without reading the body', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, '0123456789'); // 10 bytes, chunk size 4 -> 3 chunks

    const result = await splitUpload(ctx.deps, { key });
    expect(result).toEqual({ jobId, totalChunks: 3, contentLength: 10 });

    const chunks = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId)).orderBy(ingestionChunks.chunkIndex);
    expect(chunks.map((c) => [c.chunkIndex, c.byteStart, c.byteEnd, c.status])).toEqual([[0, 0, 3, 'pending'], [1, 4, 7, 'pending'], [2, 8, 9, 'pending']]);
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'processing', totalChunks: 3 });

    const messages = await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 3);
    const bodies = messages.map((m) => JSON.parse(m.Body!)).sort((a, b) => a.chunkIndex - b.chunkIndex);
    expect(bodies).toEqual([
      { jobId, chunkIndex: 0, byteStart: 0, byteEnd: 3 },
      { jobId, chunkIndex: 1, byteStart: 4, byteEnd: 7 },
      { jobId, chunkIndex: 2, byteStart: 8, byteEnd: 9 },
    ]);
  });

  it('is idempotent: a re-run re-sends only pending chunks and inserts nothing new', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, '0123456789');
    await splitUpload(ctx.deps, { key });
    await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 3);
    await ctx.deps.db.update(ingestionChunks).set({ status: 'completed' }).where(eq(ingestionChunks.chunkIndex, 0));

    const again = await splitUpload(ctx.deps, { key });
    expect(again).toEqual({ jobId, totalChunks: 3, contentLength: 10 });
    expect((await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId))).length).toBe(3);
    const messages = await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 2, 5000);
    expect(messages.map((m) => JSON.parse(m.Body!).chunkIndex).sort()).toEqual([1, 2]);
  });

  it('completes an empty file immediately', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/empty.csv`;
    await ctx.putObject(key, '');
    expect(await splitUpload(ctx.deps, { key })).toEqual({ jobId, totalChunks: 0, contentLength: 0 });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job!.status).toBe('completed');
  });

  it('returns null for a key with no job', async () => {
    const key = 'uploads/00000000-0000-0000-0000-000000000000/x.csv';
    await ctx.putObject(key, 'abc');
    expect(await splitUpload(ctx.deps, { key })).toBeNull();
  });
});
```

- [ ] **Step 5: Run to verify failure**

Run: `pnpm install && pnpm --filter @modaco/ingest test test/splitter.test.ts`
Expected: FAIL, module `../src/splitter` not found.

- [ ] **Step 6: Implement the splitter**

`apps/ingest/src/splitter.ts`:
```ts
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { and, eq } from 'drizzle-orm';
import { computeChunks, ingestionChunks, ingestionJobs, type ChunkRange } from '@modaco/core';
import { z } from 'zod';
import type { IngestDeps } from './deps';

export const chunkMessageSchema = z.object({
  jobId: z.string().uuid(),
  chunkIndex: z.number().int().min(0),
  byteStart: z.number().int().min(0),
  byteEnd: z.number().int().min(0),
});
export type ChunkMessage = z.infer<typeof chunkMessageSchema>;

const KEY_RE = /^uploads\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\//;
export function jobIdFromKey(key: string): string | null {
  return KEY_RE.exec(key)?.[1] ?? null;
}

type Deps = Pick<IngestDeps, 'db' | 's3' | 'sqs' | 'config' | 'logger'>;

async function enqueue(deps: Deps, jobId: string, chunks: ChunkRange[]): Promise<void> {
  for (let i = 0; i < chunks.length; i += 10) {
    const batch = chunks.slice(i, i + 10);
    const res = await deps.sqs.send(new SendMessageBatchCommand({
      QueueUrl: deps.config.chunkQueueUrl,
      Entries: batch.map((c) => ({
        Id: String(c.chunkIndex),
        MessageBody: JSON.stringify({ jobId, chunkIndex: c.chunkIndex, byteStart: c.byteStart, byteEnd: c.byteEnd } satisfies ChunkMessage),
      })),
    }));
    if (res.Failed?.length) throw new Error(`sqs batch send failed for chunks ${res.Failed.map((f) => f.Id).join(',')}`);
  }
}

/**
 * Splits an uploaded file into byte-range chunks using only its size (HEAD), records them, and enqueues one message per chunk.
 * Runtime is independent of file size. Safe to re-run: existing chunk rows are kept and only still-pending chunks are re-sent.
 */
export async function splitUpload(deps: Deps, input: { key: string }): Promise<{ jobId: string; totalChunks: number; contentLength: number } | null> {
  const jobId = jobIdFromKey(input.key);
  if (!jobId) { deps.logger.warn({ key: input.key }, 'ignoring object outside uploads/<jobId>/'); return null; }
  const [job] = await deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
  if (!job) { deps.logger.warn({ key: input.key, jobId }, 'no job for key; ignoring'); return null; }

  const head = await deps.s3.send(new HeadObjectCommand({ Bucket: deps.config.s3Bucket, Key: input.key }));
  const contentLength = head.ContentLength ?? 0;
  const chunks = computeChunks(contentLength, deps.config.chunkSizeBytes);

  if (chunks.length === 0) {
    await deps.db.update(ingestionJobs).set({ status: 'completed', s3Key: input.key, totalChunks: 0, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
    return { jobId, totalChunks: 0, contentLength };
  }

  await deps.db.transaction(async (tx) => {
    await tx.update(ingestionJobs).set({ status: 'splitting', s3Key: input.key, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
    await tx.insert(ingestionChunks)
      .values(chunks.map((c) => ({ jobId, chunkIndex: c.chunkIndex, byteStart: c.byteStart, byteEnd: c.byteEnd })))
      .onConflictDoNothing({ target: [ingestionChunks.jobId, ingestionChunks.chunkIndex] });
    await tx.update(ingestionJobs).set({ status: 'processing', totalChunks: chunks.length, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
  });

  const pending = await deps.db.select({ chunkIndex: ingestionChunks.chunkIndex, byteStart: ingestionChunks.byteStart, byteEnd: ingestionChunks.byteEnd })
    .from(ingestionChunks).where(and(eq(ingestionChunks.jobId, jobId), eq(ingestionChunks.status, 'pending')));
  await enqueue(deps, jobId, pending);

  deps.logger.info({ jobId, contentLength, totalChunks: chunks.length, enqueued: pending.length }, 'split complete');
  return { jobId, totalChunks: chunks.length, contentLength };
}
```

- [ ] **Step 7: Run tests**

Run: `pnpm --filter @modaco/ingest test test/splitter.test.ts`
Expected: PASS, 5 tests.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(ingest): splitter enqueues byte-range chunks from object size alone

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 13: Worker handler with streaming pricing and bulk upsert

**Files:**
- Create: `apps/ingest/src/job-state.ts`, `apps/ingest/src/worker.ts`, `apps/ingest/test/worker.test.ts`

**Interfaces:**
- Produces:
  - `markChunkCompleted(db, args: { jobId: string; chunkIndex: number; rowsProcessed: number; rowsRejected: number }): Promise<void>` (also flips the job to completed/failed when all chunks are accounted for)
  - `markChunkFailed(db, args: { jobId: string; chunkIndex: number; error: string }): Promise<{ changed: boolean }>` (no-op for a completed chunk)
  - `processChunk(deps: Pick<IngestDeps, 'db' | 's3' | 'redis' | 'config' | 'logger'>, msg: ChunkMessage): Promise<{ skipped: boolean; rowsProcessed: number; rowsRejected: number }>`

- [ ] **Step 1: Write the failing worker tests**

`apps/ingest/test/worker.test.ts`:
```ts
import { eq } from 'drizzle-orm';
import { categories, computeChunks, ingestionChunks, ingestionJobs, ingestionRejections, keys, products } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { markChunkFailed } from '../src/job-state';
import { processChunk } from '../src/worker';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ UPSERT_BATCH_SIZE: '3' }); });
afterAll(() => ctx.close());
beforeEach(() => ctx.truncateAll());

const csv = [
  'sku,name,category,vendor_price,stock',
  'S1,Belt,Accessories,10.00,5',
  'S2,"Hat, wool",Accessories,20.00,0',
  'S3,Boot,Shoes,50.00,2',
  'BAD1,,Shoes,50.00,2',
  'S4,Bag,Bags,0,1',
  'S1,Belt v2,Accessories,12.00,9',
  'S5,Scarf,Accessories,7.50,3',
  'short,row',
  'S6,Coat,Outerwear,100.00,1',
].join('\n') + '\n';
const dataRows = 9;

async function prepare(body: string, chunkSize: number) {
  const jobId = await ctx.createJob('placeholder');
  const key = `uploads/${jobId}/vendor.csv`;
  await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
  await ctx.putObject(key, body);
  const chunks = computeChunks(Buffer.byteLength(body), chunkSize);
  await ctx.deps.db.insert(ingestionChunks).values(chunks.map((c) => ({ jobId, ...c })));
  await ctx.deps.db.update(ingestionJobs).set({ status: 'processing', totalChunks: chunks.length }).where(eq(ingestionJobs.id, jobId));
  return { jobId, chunks };
}

describe('processChunk', () => {
  it('prices, upserts, rejects, and completes the job across many small chunks', async () => {
    const { jobId, chunks } = await prepare(csv, 40);
    expect(chunks.length).toBeGreaterThan(3);
    for (const c of chunks) await processChunk(ctx.deps, { jobId, ...c });

    const rows = await ctx.deps.db.select().from(products).orderBy(products.sku);
    expect(rows.map((r) => [r.sku, r.name, r.basePrice, r.stock])).toEqual([
      ['S1', 'Belt v2', '15.99', 9],   // 12.00 * 1.3 = 15.60 -> 15.99 (last occurrence wins)
      ['S2', 'Hat, wool', '26.99', 0], // 20.00 * 1.3 = 26.00 -> 26.99
      ['S3', 'Boot', '65.99', 2],      // 50 * 1.3 = 65.00 -> 65.99
      ['S5', 'Scarf', '9.99', 3],      // 7.50 * 1.3 = 9.75 -> 9.99
      ['S6', 'Coat', '130.99', 1],     // 100 * 1.3 = 130.00 -> 130.99
    ]);
    const cats = await ctx.deps.db.select().from(categories).orderBy(categories.name);
    expect(cats.map((c) => c.slug)).toEqual(['accessories', 'bags', 'outerwear', 'shoes']);

    const rejections = await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId));
    expect(rejections).toHaveLength(3);
    expect(rejections.map((r) => r.reason)).toEqual(expect.arrayContaining([
      expect.stringContaining('name'), expect.stringContaining('vendor_price'), expect.stringContaining('expected 5 fields'),
    ]));

    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'completed', completedChunks: chunks.length, failedChunks: 0, rowsRejected: 3 });
    expect(job!.rowsProcessed + job!.rowsRejected).toBe(dataRows);

    const accessories = cats.find((c) => c.slug === 'accessories')!;
    expect(Number(await ctx.deps.redis.get(keys.categoryVersion(accessories.id)))).toBeGreaterThan(0);
    expect(Number(await ctx.deps.redis.get(keys.allVersion()))).toBeGreaterThan(0);
    const s1 = rows.find((r) => r.sku === 'S1')!;
    expect(await ctx.deps.redis.get(keys.stock(s1.id))).toBe('9');
  });

  it('is a no-op when the chunk is already completed', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000);
    const first = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(first.skipped).toBe(false);
    const second = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(second).toEqual({ skipped: true, rowsProcessed: first.rowsProcessed, rowsRejected: first.rowsRejected });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job!.completedChunks).toBe(1);
  });

  it('uses category pricing overrides and updates existing products', async () => {
    await ctx.deps.db.insert(categories).values({ name: 'Shoes', slug: 'shoes', marginPct: '0', priceFloor: '60.00', priceCeiling: '61.00' });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS3,Old Boot,Shoes,50.00,2\n', 10_000);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    const [boot] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'S3'));
    expect(boot).toMatchObject({ name: 'Old Boot', basePrice: '60.00', stock: 2 });
  });

  it('rejects a row whose category name collides with an existing slug', async () => {
    await ctx.deps.db.insert(categories).values({ name: 'Shoes', slug: 'shoes' });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS9,Sneaker,shoes,50.00,2\n', 10_000);
    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r).toMatchObject({ rowsProcessed: 0, rowsRejected: 1 });
    const [rej] = await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId));
    expect(rej!.reason).toContain('collides');
  });

  it('records the error, leaves the chunk retryable, and rethrows on failure', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000);
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: `uploads/${jobId}/missing.csv` }).where(eq(ingestionJobs.id, jobId));
    await expect(processChunk(ctx.deps, { jobId, ...chunks[0]! })).rejects.toThrow();
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunk).toMatchObject({ status: 'processing', attempts: 1 });
    expect(chunk!.error).toBeTruthy();
  });

  it('markChunkFailed fails the job once and ignores completed chunks', async () => {
    const { jobId, chunks } = await prepare(csv, 60);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 0, error: 'x' })).toEqual({ changed: false });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 1, error: 'boom' })).toEqual({ changed: true });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 1, error: 'boom again' })).toEqual({ changed: false });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', failedChunks: 1 });
    expect(job!.error).toContain('chunk 1');
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter @modaco/ingest test test/worker.test.ts`
Expected: FAIL, modules not found.

- [ ] **Step 3: Implement job state transitions**

`apps/ingest/src/job-state.ts`:
```ts
import { and, eq, sql } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs, type Db } from '@modaco/core';

/** Chunk done: write its counts and atomically advance the job. Flips the job status when every chunk is accounted for. */
export async function markChunkCompleted(db: Db, args: { jobId: string; chunkIndex: number; rowsProcessed: number; rowsRejected: number }): Promise<void> {
  await db.transaction(async (tx) => {
    await tx.update(ingestionChunks)
      .set({ status: 'completed', rowsProcessed: args.rowsProcessed, rowsRejected: args.rowsRejected, error: null, updatedAt: new Date() })
      .where(and(eq(ingestionChunks.jobId, args.jobId), eq(ingestionChunks.chunkIndex, args.chunkIndex)));
    const [job] = await tx.update(ingestionJobs).set({
      completedChunks: sql`${ingestionJobs.completedChunks} + 1`,
      rowsProcessed: sql`${ingestionJobs.rowsProcessed} + ${args.rowsProcessed}`,
      rowsRejected: sql`${ingestionJobs.rowsRejected} + ${args.rowsRejected}`,
      updatedAt: new Date(),
    }).where(eq(ingestionJobs.id, args.jobId)).returning();
    if (job && job.completedChunks + job.failedChunks >= job.totalChunks) {
      await tx.update(ingestionJobs).set({ status: job.failedChunks > 0 ? 'failed' : 'completed', updatedAt: new Date() }).where(eq(ingestionJobs.id, args.jobId));
    }
  });
}

/** Dead-letter path: a chunk that exhausted its retries. The job is failed immediately, never left silently partial. */
export async function markChunkFailed(db: Db, args: { jobId: string; chunkIndex: number; error: string }): Promise<{ changed: boolean }> {
  return db.transaction(async (tx) => {
    const [chunk] = await tx.select().from(ingestionChunks)
      .where(and(eq(ingestionChunks.jobId, args.jobId), eq(ingestionChunks.chunkIndex, args.chunkIndex))).for('update');
    if (!chunk || chunk.status === 'completed' || chunk.status === 'failed') return { changed: false };
    await tx.update(ingestionChunks).set({ status: 'failed', error: args.error, updatedAt: new Date() }).where(eq(ingestionChunks.id, chunk.id));
    await tx.update(ingestionJobs).set({
      status: 'failed',
      failedChunks: sql`${ingestionJobs.failedChunks} + 1`,
      error: `chunk ${args.chunkIndex}: ${args.error}`.slice(0, 2000),
      updatedAt: new Date(),
    }).where(eq(ingestionJobs.id, args.jobId));
    return { changed: true };
  });
}
```

- [ ] **Step 4: Implement the worker**

`apps/ingest/src/worker.ts`:
```ts
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { and, eq, inArray, sql } from 'drizzle-orm';
import type { Readable } from 'node:stream';
import {
  bumpVersions, categories, categoryPricingFromRow, DEFAULT_CATEGORY_PRICING, fromCents, ingestionChunks, ingestionJobs,
  ingestionRejections, keys, ownedLines, parseCsvLine, priceVendorRow, products, rangeFor, rowFromFields, slugify,
  STOCK_TTL_SECONDS, VENDOR_COLUMNS, type CategoryPricing, type Db, type PricedRow, type Redis,
} from '@modaco/core';
import type { Logger } from 'pino';
import type { IngestDeps } from './deps';
import { markChunkCompleted } from './job-state';
import type { ChunkMessage } from './splitter';

type Deps = Pick<IngestDeps, 'db' | 's3' | 'redis' | 'config' | 'logger'>;

interface PendingLine { lineNumber: number; raw: string }
interface Rejection { lineNumber: number; rawLine: string; reason: string }

/** Accumulates parsed lines and writes them in bounded batches. Memory never exceeds one batch. */
class BatchWriter {
  private pending: PendingLine[] = [];
  rowsProcessed = 0;
  rowsRejected = 0;

  constructor(
    private readonly db: Db, private readonly redis: Redis, private readonly logger: Logger,
    private readonly jobId: string, private readonly chunkIndex: number, private readonly batchSize: number,
  ) {}

  async add(line: PendingLine): Promise<void> {
    this.pending.push(line);
    if (this.pending.length >= this.batchSize) await this.flush();
  }

  async flush(): Promise<void> {
    if (this.pending.length === 0) return;
    const batch = this.pending;
    this.pending = [];

    const rejections: Rejection[] = [];
    const parsed: Array<{ lineNumber: number; raw: string; row: Record<string, string> }> = [];
    for (const { lineNumber, raw } of batch) {
      const fields = parseCsvLine(raw);
      const row = rowFromFields(fields);
      if (!row) rejections.push({ lineNumber, rawLine: raw, reason: `malformed: expected ${VENDOR_COLUMNS.length} fields, got ${fields.length}` });
      else parsed.push({ lineNumber, raw, row });
    }

    const pricingByName = await this.ensureCategories([...new Set(parsed.map((p) => p.row.category!.trim()).filter(Boolean))]);
    const priced: PricedRow[] = [];
    for (const { lineNumber, raw, row } of parsed) {
      const outcome = priceVendorRow(row, (name) => pricingByName.get(name)?.pricing ?? DEFAULT_CATEGORY_PRICING);
      if (!outcome.ok) { rejections.push({ lineNumber, rawLine: raw, reason: outcome.reason }); continue; }
      // Two distinct names that slugify identically (e.g. "Shoes" and "shoes") cannot both exist; the loser is rejected.
      if (!pricingByName.has(outcome.row.category)) { rejections.push({ lineNumber, rawLine: raw, reason: `category '${outcome.row.category}' collides with an existing category slug` }); continue; }
      priced.push(outcome.row);
    }

    // Postgres refuses to update the same row twice in one INSERT ... ON CONFLICT; last occurrence of a SKU wins.
    const bySku = new Map<string, PricedRow>();
    for (const r of priced) bySku.set(r.sku, r);
    const unique = [...bySku.values()];

    const touchedCategories = new Set<number>();
    const written: Array<{ id: number; stock: number }> = [];
    await this.db.transaction(async (tx) => {
      if (unique.length > 0) {
        const rows = await tx.insert(products).values(unique.map((r) => {
          const categoryId = pricingByName.get(r.category)!.id;
          touchedCategories.add(categoryId);
          return { sku: r.sku, name: r.name, categoryId, basePrice: fromCents(r.basePriceCents), stock: r.stock };
        })).onConflictDoUpdate({
          target: products.sku,
          set: {
            name: sql.raw(`excluded.${products.name.name}`),
            categoryId: sql.raw(`excluded.${products.categoryId.name}`),
            basePrice: sql.raw(`excluded.${products.basePrice.name}`),
            stock: sql.raw(`excluded.${products.stock.name}`),
            updatedAt: sql`now()`,
          },
        }).returning({ id: products.id, stock: products.stock });
        written.push(...rows);
      }
      if (rejections.length > 0) {
        await tx.insert(ingestionRejections).values(rejections.map((r) => ({ jobId: this.jobId, chunkIndex: this.chunkIndex, ...r })));
      }
    });

    this.rowsProcessed += priced.length;
    this.rowsRejected += rejections.length;
    await this.publish(written, [...touchedCategories]);
  }

  private async ensureCategories(names: string[]): Promise<Map<string, { id: number; pricing: CategoryPricing }>> {
    const out = new Map<string, { id: number; pricing: CategoryPricing }>();
    if (names.length === 0) return out;
    await this.db.insert(categories).values(names.map((name) => ({ name, slug: slugify(name) }))).onConflictDoNothing();
    const rows = await this.db.select().from(categories).where(inArray(categories.name, names));
    for (const c of rows) out.set(c.name, { id: c.id, pricing: categoryPricingFromRow(c) });
    return out;
  }

  /** After commit: stock counters and version bumps so the storefront sees new prices at once. Never throws. */
  private async publish(written: Array<{ id: number; stock: number }>, categoryIds: number[]): Promise<void> {
    const log = (msg: string, err: unknown) => this.logger.error({ err, jobId: this.jobId, chunkIndex: this.chunkIndex }, msg);
    if (written.length > 0) {
      const pipe = this.redis.pipeline();
      for (const w of written) pipe.set(keys.stock(w.id), String(w.stock), 'EX', STOCK_TTL_SECONDS);
      await pipe.exec().catch((err) => log('stock counter publish failed', err));
    }
    if (categoryIds.length > 0) {
      await bumpVersions(this.redis, [...categoryIds.map(keys.categoryVersion), keys.allVersion()], log);
    }
  }
}

/**
 * Processes exactly one byte-range chunk: streams it from S3, prices every owned line, upserts in batches,
 * then records completion. Idempotent: a redelivered message for a completed chunk exits immediately.
 */
export async function processChunk(deps: Deps, msg: ChunkMessage): Promise<{ skipped: boolean; rowsProcessed: number; rowsRejected: number }> {
  const [chunk] = await deps.db.select().from(ingestionChunks)
    .where(and(eq(ingestionChunks.jobId, msg.jobId), eq(ingestionChunks.chunkIndex, msg.chunkIndex)));
  if (!chunk) throw new Error(`chunk ${msg.jobId}/${msg.chunkIndex} not found`);
  if (chunk.status === 'completed') return { skipped: true, rowsProcessed: chunk.rowsProcessed, rowsRejected: chunk.rowsRejected };
  const [job] = await deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, msg.jobId));
  if (!job) throw new Error(`job ${msg.jobId} not found`);

  await deps.db.update(ingestionChunks)
    .set({ status: 'processing', attempts: sql`${ingestionChunks.attempts} + 1`, updatedAt: new Date() })
    .where(eq(ingestionChunks.id, chunk.id));

  const writer = new BatchWriter(deps.db, deps.redis, deps.logger, msg.jobId, msg.chunkIndex, deps.config.upsertBatchSize);
  try {
    const { rangeStart, rangeEnd } = rangeFor(chunk);
    const obj = await deps.s3.send(new GetObjectCommand({ Bucket: deps.config.s3Bucket, Key: job.s3Key, Range: `bytes=${rangeStart}-${rangeEnd}` }));
    const body = obj.Body as Readable;

    let skipHeader = chunk.byteStart === 0;
    let lineNumber = 0;
    for await (const { line } of ownedLines(body, chunk, rangeStart)) {
      if (skipHeader) { skipHeader = false; continue; }
      lineNumber++;
      if (line.trim() === '') continue;
      await writer.add({ lineNumber, raw: line });
    }
    await writer.flush();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await deps.db.update(ingestionChunks).set({ error: message.slice(0, 2000), updatedAt: new Date() }).where(eq(ingestionChunks.id, chunk.id));
    deps.logger.error({ err, jobId: msg.jobId, chunkIndex: msg.chunkIndex }, 'chunk failed; will be retried by the queue');
    throw err;
  }

  await markChunkCompleted(deps.db, { jobId: msg.jobId, chunkIndex: msg.chunkIndex, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected });
  deps.logger.info({ jobId: msg.jobId, chunkIndex: msg.chunkIndex, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected }, 'chunk complete');
  return { skipped: false, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected };
}
```

- [ ] **Step 5: Run tests**

Run: `pnpm --filter @modaco/ingest test test/worker.test.ts`
Expected: PASS, 6 tests. If the first test's price expectations differ, check the rounding chain by hand: `vendor * 1.3` in cents, then `roundUpTo99`, then clamp with defaults 99..9999999.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(ingest): streaming chunk worker with batched pricing, upsert, and job completion

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 14: Lambda adapters, dead-letter handler, queue polling, local runner, bundle, SAM

**Files:**
- Create: `apps/ingest/src/dlq.ts`, `apps/ingest/src/handlers/lambda.ts`, `apps/ingest/src/invoke.ts`, `apps/ingest/src/queue.ts`, `apps/ingest/src/runner.ts`, `apps/ingest/build.mjs`, `infra/template.yaml`, `apps/ingest/test/queue.test.ts`

**Interfaces:**
- Produces:
  - `handleDeadLetter(deps: Pick<IngestDeps, 'db' | 'logger'>, msg: ChunkMessage, reason: string): Promise<void>`
  - Lambda exports `splitter: S3Handler`, `worker: SQSHandler`, `deadLetter: SQSHandler`
  - `pollOnce(sqs: SQSClient, queueUrl: string, handle: (message: Message) => Promise<void>, opts?: { waitSeconds?: number; max?: number }): Promise<{ received: number; succeeded: number; failed: number }>` (deletes a message only when `handle` resolves)
  - `sqsEventFrom(message: Message): SQSEvent`
  - `pnpm --filter @modaco/ingest runner` starts the local runner; `pnpm build:lambda` writes `apps/ingest/dist/lambda.mjs`

- [ ] **Step 1: Dead-letter handler and Lambda adapters**

`apps/ingest/src/dlq.ts`:
```ts
import type { IngestDeps } from './deps';
import { markChunkFailed } from './job-state';
import type { ChunkMessage } from './splitter';

export async function handleDeadLetter(deps: Pick<IngestDeps, 'db' | 'logger'>, msg: ChunkMessage, reason: string): Promise<void> {
  const { changed } = await markChunkFailed(deps.db, { jobId: msg.jobId, chunkIndex: msg.chunkIndex, error: reason });
  deps.logger.error({ jobId: msg.jobId, chunkIndex: msg.chunkIndex, reason, changed }, 'chunk dead-lettered');
}
```

`apps/ingest/src/handlers/lambda.ts`:
```ts
import type { S3Event, S3Handler, SQSHandler } from 'aws-lambda';
import { getDeps } from '../deps';
import { handleDeadLetter } from '../dlq';
import { chunkMessageSchema, splitUpload } from '../splitter';
import { processChunk } from '../worker';

const decodeKey = (key: string) => decodeURIComponent(key.replace(/\+/g, ' '));

export const splitter: S3Handler = async (event: Partial<S3Event>) => {
  const deps = await getDeps();
  for (const record of event.Records ?? []) {
    await splitUpload(deps, { key: decodeKey(record.s3.object.key) });
  }
};

export const worker: SQSHandler = async (event) => {
  const deps = await getDeps();
  for (const record of event.Records) {
    await processChunk(deps, chunkMessageSchema.parse(JSON.parse(record.body)));
  }
};

export const deadLetter: SQSHandler = async (event) => {
  const deps = await getDeps();
  for (const record of event.Records) {
    const receives = record.attributes?.ApproximateReceiveCount ?? '?';
    await handleDeadLetter(deps, chunkMessageSchema.parse(JSON.parse(record.body)), `exceeded max receive count (receives=${receives})`);
  }
};
```

`apps/ingest/src/invoke.ts` (child-process entry used by the local runner; reads the event from stdin):
```ts
import { deadLetter, splitter, worker } from './handlers/lambda';

const handlers = { splitter, worker, deadLetter } as const;
const name = process.argv[2] as keyof typeof handlers;
if (!handlers[name]) { console.error(`unknown handler ${name}`); process.exit(2); }

const chunks: Buffer[] = [];
for await (const c of process.stdin) chunks.push(c as Buffer);
const event = JSON.parse(Buffer.concat(chunks).toString('utf8'));

try {
  await (handlers[name] as (e: unknown, c: unknown, cb: () => void) => Promise<void>)(event, {}, () => {});
  process.exit(0);
} catch (err) {
  console.error(err);
  process.exit(1);
}
```

- [ ] **Step 2: Write the failing queue test**

`apps/ingest/test/queue.test.ts`:
```ts
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { pollOnce, sqsEventFrom } from '../src/queue';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest(); });
afterAll(() => ctx.close());
beforeEach(() => ctx.drainQueue(ctx.deps.config.dlqUrl));

describe('pollOnce', () => {
  it('deletes handled messages and leaves failed ones for redelivery', async () => {
    const url = ctx.deps.config.dlqUrl; // any queue without consumers works for this test
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: 'ok' }));
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: 'fail' }));
    const seen: string[] = [];
    const r = await pollOnce(ctx.deps.sqs, url, async (m) => { seen.push(m.Body!); if (m.Body === 'fail') throw new Error('nope'); }, { waitSeconds: 1 });
    expect(seen.sort()).toEqual(['fail', 'ok']);
    expect(r).toEqual({ received: 2, succeeded: 1, failed: 1 });
    const empty = await pollOnce(ctx.deps.sqs, url, async () => {}, { waitSeconds: 1 });
    expect(empty.received).toBe(0); // 'fail' is invisible until its visibility timeout elapses
  });

  it('wraps a message as an SQSEvent', () => {
    const ev = sqsEventFrom({ MessageId: 'm1', ReceiptHandle: 'rh', Body: '{"a":1}', Attributes: { ApproximateReceiveCount: '2' } });
    expect(ev.Records).toHaveLength(1);
    expect(ev.Records[0]).toMatchObject({ messageId: 'm1', body: '{"a":1}', attributes: { ApproximateReceiveCount: '2' } });
  });
});
```

- [ ] **Step 3: Run to verify failure**

Run: `pnpm --filter @modaco/ingest test test/queue.test.ts`
Expected: FAIL, module not found.

- [ ] **Step 4: Implement queue polling and the runner**

`apps/ingest/src/queue.ts`:
```ts
import { DeleteMessageCommand, ReceiveMessageCommand, type Message, type SQSClient } from '@aws-sdk/client-sqs';
import type { SQSEvent } from 'aws-lambda';

/** One receive cycle: handle up to `max` messages concurrently, delete each only after its handler resolves. */
export async function pollOnce(
  sqs: SQSClient, queueUrl: string, handle: (message: Message) => Promise<void>,
  opts: { waitSeconds?: number; max?: number } = {},
): Promise<{ received: number; succeeded: number; failed: number }> {
  const res = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: queueUrl, MaxNumberOfMessages: opts.max ?? 10, WaitTimeSeconds: opts.waitSeconds ?? 5,
    MessageSystemAttributeNames: ['ApproximateReceiveCount'],
  }));
  const messages = res.Messages ?? [];
  const results = await Promise.allSettled(messages.map(async (m) => {
    await handle(m);
    await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle! }));
  }));
  const succeeded = results.filter((r) => r.status === 'fulfilled').length;
  return { received: messages.length, succeeded, failed: messages.length - succeeded };
}

export function sqsEventFrom(message: Message): SQSEvent {
  return {
    Records: [{
      messageId: message.MessageId ?? '', receiptHandle: message.ReceiptHandle ?? '', body: message.Body ?? '',
      attributes: {
        ApproximateReceiveCount: message.Attributes?.ApproximateReceiveCount ?? '1',
        SentTimestamp: '', SenderId: '', ApproximateFirstReceiveTimestamp: '',
      },
      messageAttributes: {}, md5OfBody: '', eventSource: 'aws:sqs', eventSourceARN: '', awsRegion: '',
    }],
  };
}
```

`apps/ingest/src/runner.ts`:
```ts
import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { createSqs } from './aws';
import { loadIngestConfig } from './config';
import { pollOnce, sqsEventFrom } from './queue';

/**
 * Local stand-in for the Lambda service. Every invocation runs in a fresh child process with a hard timeout
 * (SIGKILL on overrun, like a Lambda timeout) and a V8 heap cap (like the Lambda memory limit).
 */
const config = loadIngestConfig();
const logger = pino({ level: config.logLevel });
const sqs = createSqs(config);
const invokePath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'invoke.ts');

function invoke(handler: 'splitter' | 'worker' | 'deadLetter', event: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const child = execFile('tsx', [invokePath, handler], {
      timeout: config.lambdaTimeoutMs,
      killSignal: 'SIGKILL',
      maxBuffer: 16 * 1024 * 1024,
      env: { ...process.env, NODE_OPTIONS: `--max-old-space-size=${config.lambdaMemoryMb}` },
    }, (err, _stdout, stderr) => {
      const ms = Date.now() - started;
      if (err) {
        const why = err.killed ? `timeout after ${config.lambdaTimeoutMs}ms` : `exit ${err.code}`;
        logger.error({ handler, ms, why, stderr: stderr.slice(-2000) }, 'invocation failed');
        reject(new Error(`${handler}: ${why}`));
      } else {
        logger.info({ handler, ms }, 'invocation ok');
        resolve();
      }
    });
    child.stdin!.end(JSON.stringify(event));
  });
}

let stopping = false;
process.on('SIGINT', () => { stopping = true; });
process.on('SIGTERM', () => { stopping = true; });

async function loop(name: string, queueUrl: string, handle: (m: import('@aws-sdk/client-sqs').Message) => Promise<void>) {
  logger.info({ name, queueUrl }, 'polling');
  while (!stopping) {
    try {
      await pollOnce(sqs, queueUrl, handle);
    } catch (err) {
      logger.error({ err, name }, 'poll failed; backing off');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

await Promise.all([
  loop('s3-events', config.s3EventsQueueUrl, async (m) => {
    const body = JSON.parse(m.Body ?? '{}');
    if (!Array.isArray(body.Records)) return; // LocalStack sends an s3:TestEvent on configuration
    await invoke('splitter', body);
  }),
  loop('chunks', config.chunkQueueUrl, (m) => invoke('worker', sqsEventFrom(m))),
  loop('dead-letter', config.dlqUrl, (m) => invoke('deadLetter', sqsEventFrom(m))),
]);
```

- [ ] **Step 5: Run the queue test**

Run: `pnpm --filter @modaco/ingest test test/queue.test.ts`
Expected: PASS, 2 tests.

- [ ] **Step 6: Bundle and SAM template**

`apps/ingest/build.mjs`:
```js
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
```

`infra/template.yaml`:
```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: ModaCo vendor ingestion (splitter, chunk worker, dead-letter handler)

Parameters:
  DatabaseUrl: { Type: String, NoEcho: true }
  RedisUrl: { Type: String, NoEcho: true }
  ChunkSizeBytes: { Type: Number, Default: 4194304 }
  UpsertBatchSize: { Type: Number, Default: 1000 }
  WorkerReservedConcurrency: { Type: Number, Default: 10 }

Globals:
  Function:
    Runtime: nodejs22.x
    Architectures: [arm64]
    MemorySize: 256
    CodeUri: ../apps/ingest/dist
    Environment:
      Variables:
        DATABASE_URL: !Ref DatabaseUrl
        REDIS_URL: !Ref RedisUrl
        S3_BUCKET: !Sub '${AWS::StackName}-vendor-uploads'
        CHUNK_SIZE_BYTES: !Ref ChunkSizeBytes
        UPSERT_BATCH_SIZE: !Ref UpsertBatchSize
        CHUNK_QUEUE_URL: !Ref ChunkQueue

Resources:
  UploadsBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub '${AWS::StackName}-vendor-uploads'

  DeadLetterQueue:
    Type: AWS::SQS::Queue
    Properties:
      MessageRetentionPeriod: 1209600

  ChunkQueue:
    Type: AWS::SQS::Queue
    Properties:
      VisibilityTimeout: 360
      RedrivePolicy:
        deadLetterTargetArn: !GetAtt DeadLetterQueue.Arn
        maxReceiveCount: 3

  SplitterFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: lambda.splitter
      Timeout: 30
      Policies:
        - S3ReadPolicy: { BucketName: !Sub '${AWS::StackName}-vendor-uploads' }
        - SQSSendMessagePolicy: { QueueName: !GetAtt ChunkQueue.QueueName }
      Events:
        Upload:
          Type: S3
          Properties:
            Bucket: !Ref UploadsBucket
            Events: s3:ObjectCreated:*
            Filter:
              S3Key:
                Rules: [{ Name: prefix, Value: uploads/ }]

  WorkerFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: lambda.worker
      Timeout: 60
      ReservedConcurrentExecutions: !Ref WorkerReservedConcurrency
      Policies:
        - S3ReadPolicy: { BucketName: !Sub '${AWS::StackName}-vendor-uploads' }
      Events:
        Chunks:
          Type: SQS
          Properties:
            Queue: !GetAtt ChunkQueue.Arn
            BatchSize: 1

  DeadLetterFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: lambda.deadLetter
      Timeout: 30
      Events:
        Dead:
          Type: SQS
          Properties:
            Queue: !GetAtt DeadLetterQueue.Arn
            BatchSize: 1

Outputs:
  UploadsBucketName: { Value: !Ref UploadsBucket }
  ChunkQueueUrl: { Value: !Ref ChunkQueue }
  DeadLetterQueueUrl: { Value: !Ref DeadLetterQueue }
```

Run: `pnpm build:lambda && ls -la apps/ingest/dist`
Expected: `lambda.mjs` and `lambda.mjs.map` exist, no esbuild errors (warnings about `pg-native` are fine since it is external).

Run: `pnpm --filter @modaco/ingest typecheck`
Expected: no errors.

- [ ] **Step 7: Smoke the runner manually**

Run in one terminal: `pnpm dev:runner`
Expected: three "polling" log lines and no errors within 10 seconds. Stop it with Ctrl-C.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(ingest): lambda adapters, dead-letter handler, local runner with timeout and memory cap, SAM template

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 15: End-to-end ingestion test through LocalStack

**Files:**
- Create: `apps/ingest/test/end-to-end.test.ts`

**Interfaces:**
- Consumes: `splitUpload`, `processChunk`, `handleDeadLetter`, `pollOnce`, `chunkMessageSchema`, test helpers.

- [ ] **Step 1: Write the end-to-end test**

`apps/ingest/test/end-to-end.test.ts`:
```ts
import { count, eq } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs, keys, products } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { handleDeadLetter } from '../src/dlq';
import { pollOnce } from '../src/queue';
import { chunkMessageSchema, splitUpload } from '../src/splitter';
import { processChunk } from '../src/worker';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ CHUNK_SIZE_BYTES: '4096', UPSERT_BATCH_SIZE: '100' }); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  await Promise.all([ctx.deps.config.s3EventsQueueUrl, ctx.deps.config.chunkQueueUrl, ctx.deps.config.dlqUrl].map((u) => ctx.drainQueue(u)));
});

function vendorCsv(rows: number): { body: string; validSkus: number; badRows: number } {
  const cats = ['Accessories', 'Shoes', 'Bags'];
  const lines = ['sku,name,category,vendor_price,stock'];
  let badRows = 0;
  for (let i = 0; i < rows; i++) {
    if (i % 100 === 7) { lines.push(`BAD-${i},,${cats[i % 3]},1.00,1`); badRows++; continue; }
    lines.push(`SKU-${String(i).padStart(6, '0')},"Item ${i}, deluxe",${cats[i % 3]},${(1 + (i % 500)).toFixed(2)},${i % 50}`);
  }
  return { body: lines.join('\n') + '\n', validSkus: rows - badRows, badRows };
}

async function untilJob(jobId: string, pred: (j: typeof ingestionJobs.$inferSelect) => boolean, driver: () => Promise<unknown>, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    if (job && pred(job)) return job;
    if (Date.now() > deadline) throw new Error(`timeout waiting for job; last state ${JSON.stringify(job)}`);
    await driver();
  }
}

const driveS3Events = () => pollOnce(ctx.deps.sqs, ctx.deps.config.s3EventsQueueUrl, async (m) => {
  const body = JSON.parse(m.Body ?? '{}');
  for (const rec of body.Records ?? []) await splitUpload(ctx.deps, { key: decodeURIComponent(rec.s3.object.key.replace(/\+/g, ' ')) });
}, { waitSeconds: 1 });

const driveChunks = () => pollOnce(ctx.deps.sqs, ctx.deps.config.chunkQueueUrl, async (m) => {
  await processChunk(ctx.deps, chunkMessageSchema.parse(JSON.parse(m.Body!)));
}, { waitSeconds: 1 });

describe('ingestion end to end', () => {
  it('uploads a file, splits via the S3 notification, processes every chunk, and lands every row exactly once', async () => {
    const { body, validSkus, badRows } = vendorCsv(2500);
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, body);

    const split = await untilJob(jobId, (j) => j.status === 'processing', driveS3Events);
    expect(split.totalChunks).toBe(Math.ceil(Buffer.byteLength(body) / 4096));

    const done = await untilJob(jobId, (j) => j.status === 'completed' || j.status === 'failed', driveChunks);
    expect(done.status).toBe('completed');
    expect(done.completedChunks).toBe(done.totalChunks);
    expect(done.rowsRejected).toBe(badRows);
    expect(done.rowsProcessed).toBe(validSkus);

    const [{ n }] = await ctx.deps.db.select({ n: count() }).from(products);
    expect(Number(n)).toBe(validSkus);
    const chunks = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunks.every((c) => c.status === 'completed')).toBe(true);

    const [sample] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'SKU-000010'));
    expect(sample).toMatchObject({ name: 'Item 10, deluxe', basePrice: '14.99', stock: 10 }); // 11.00 * 1.3 = 14.30 -> 14.99
    expect(Number(await ctx.deps.redis.get(keys.allVersion()))).toBeGreaterThan(0);
  });

  it('fails the job through the dead-letter path when a chunk keeps failing', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, vendorCsv(50).body);
    await untilJob(jobId, (j) => j.status === 'processing', driveS3Events);
    // Simulate the object disappearing so every attempt fails.
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: `uploads/${jobId}/gone.csv` }).where(eq(ingestionJobs.id, jobId));
    const r = await driveChunks();
    expect(r.failed).toBe(1);
    // Three failed receives would route the message to the DLQ; call the handler directly since the visibility timeout is 360s.
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    await handleDeadLetter(ctx.deps, { jobId, chunkIndex: chunk!.chunkIndex, byteStart: chunk!.byteStart, byteEnd: chunk!.byteEnd }, 'exceeded max receive count');
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', failedChunks: 1 });
  });
});
```

- [ ] **Step 2: Run**

Run: `pnpm --filter @modaco/ingest test test/end-to-end.test.ts`
Expected: PASS, 2 tests, under a minute. If the first `untilJob` times out, check that the LocalStack notification is configured: `docker compose exec localstack awslocal s3api get-bucket-notification-configuration --bucket modaco-vendor-uploads` must show the queue ARN. If it is empty, the init script did not run; `docker compose down -v && docker compose up -d` and retry.

- [ ] **Step 3: Run the whole suite**

Run: `pnpm test && pnpm typecheck`
Expected: every package PASS, no type errors.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(ingest): end-to-end ingestion through LocalStack S3 notification and SQS

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 16: Seed, vendor file generator, and demo scripts

**Files:**
- Create: `scripts/seed.ts`, `scripts/generate-vendor-file.ts`, `scripts/demo-ingest.ts`, `scripts/demo-flash-sale.ts`

**Interfaces:**
- Consumes: `@modaco/core` (`createDb`, tables, `slugify`), the HTTP API.
- Produces: CLI scripts wired to the root `package.json` scripts from Task 1.

- [ ] **Step 1: Seed**

`scripts/seed.ts`:
```ts
import { categories, createDb, products, runMigrations, slugify, TEST_DATABASE_URL } from '@modaco/core';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });
await runMigrations(db);

const names = ['Accessories', 'Shoes', 'Bags', 'Outerwear'];
await db.insert(categories).values(names.map((name) => ({ name, slug: slugify(name) }))).onConflictDoNothing();
const cats = await db.select().from(categories);

const rows = cats.flatMap((c) => Array.from({ length: 50 }, (_, i) => ({
  sku: `${c.slug.toUpperCase()}-${String(i + 1).padStart(3, '0')}`,
  name: `${c.name} item ${i + 1}`,
  categoryId: c.id,
  basePrice: (9.99 + i * 3).toFixed(2),
  stock: (i * 7) % 40,
})));
await db.insert(products).values(rows).onConflictDoNothing();
console.log(`seeded ${cats.length} categories, ${rows.length} products`);
await close();
```

- [ ] **Step 2: Vendor file generator**

`scripts/generate-vendor-file.ts`:
```ts
import { createWriteStream, mkdirSync } from 'node:fs';
import { once } from 'node:events';
import path from 'node:path';

const args = Object.fromEntries(process.argv.slice(2).map((a) => a.replace(/^--/, '').split('=')));
const rows = Number(args.rows ?? 500_000);
const out = args.out ?? 'tmp/vendor-500k.csv';
const badRatio = Number(args['bad-ratio'] ?? 0.001);

const categories = ['Accessories', 'Shoes', 'Bags', 'Outerwear', 'Dresses', 'Knitwear', 'Denim', 'Sportswear'];
let seed = 42;
const rand = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 2 ** 32; };

mkdirSync(path.dirname(out), { recursive: true });
const stream = createWriteStream(out);
stream.write('sku,name,category,vendor_price,stock\n');
for (let i = 0; i < rows; i++) {
  const cat = categories[i % categories.length];
  const line = rand() < badRatio
    ? `SKU-${String(i).padStart(7, '0')},,${cat},-1,1\n`
    : `SKU-${String(i).padStart(7, '0')},"${cat} style ${i}, vendor line",${cat},${(1 + rand() * 499).toFixed(2)},${Math.floor(rand() * 1000)}\n`;
  if (!stream.write(line)) await once(stream, 'drain');
}
stream.end();
await once(stream, 'finish');
console.log(`wrote ${rows} rows to ${out}`);
```

- [ ] **Step 3: Ingestion demo**

`scripts/demo-ingest.ts`:
```ts
import { readFileSync, statSync } from 'node:fs';

const api = process.env.API_URL ?? 'http://localhost:3000';
const file = process.argv[2] ?? 'tmp/vendor-500k.csv';
const started = Date.now();

const job = await (await fetch(`${api}/ingestion/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ filename: 'vendor.csv' }) })).json();
console.log(`job ${job.jobId}: uploading ${file} (${(statSync(file).size / 1_048_576).toFixed(1)} MB)`);
const put = await fetch(job.uploadUrl, { method: 'PUT', body: readFileSync(file) });
if (!put.ok) throw new Error(`upload failed: ${put.status}`);
console.log(`uploaded in ${((Date.now() - started) / 1000).toFixed(1)}s; waiting for the splitter and workers`);

for (;;) {
  const j = await (await fetch(`${api}/ingestion/jobs/${job.jobId}`)).json();
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`[${elapsed}s] ${j.status} chunks ${j.completedChunks}/${j.totalChunks} rows ${j.rowsProcessed} rejected ${j.rowsRejected}`);
  if (j.status === 'completed' || j.status === 'failed') {
    const total = j.rowsProcessed + j.rowsRejected;
    console.log(`${j.status}: ${total} rows in ${elapsed}s (${Math.round(total / Number(elapsed))} rows/s)${j.error ? `\nerror: ${j.error}` : ''}`);
    break;
  }
  await new Promise((r) => setTimeout(r, 1000));
}
```

- [ ] **Step 4: Flash sale demo**

`scripts/demo-flash-sale.ts`:
```ts
const api = process.env.API_URL ?? 'http://localhost:3000';
const slug = process.argv[2] ?? 'accessories';
const durationMs = Number(process.env.DURATION_MS ?? 15_000);
const concurrency = Number(process.env.CONCURRENCY ?? 50);
const flipAtMs = Math.floor(durationMs / 3);

const first = await (await fetch(`${api}/products?category=${slug}&pageSize=1`)).json();
const categoryId = first.items[0]?.category.id;
if (!categoryId) throw new Error(`no products in category ${slug}; run pnpm seed or ingest first`);

const before: number[] = []; const after: number[] = [];
let flipped = false; let errors = 0; let priceBefore = ''; let priceAfter = '';
const started = Date.now();

async function worker() {
  while (Date.now() - started < durationMs) {
    const page = 1 + Math.floor(Math.random() * 5);
    const t = performance.now();
    const res = await fetch(`${api}/products?category=${slug}&page=${page}&pageSize=20`);
    const ms = performance.now() - t;
    if (!res.ok) { errors++; continue; }
    (flipped ? after : before).push(ms);
    if (page === 1) {
      const body = await res.json();
      if (!flipped) priceBefore = body.items[0].effectivePrice; else priceAfter = body.items[0].effectivePrice;
    }
  }
}

const flip = (async () => {
  await new Promise((r) => setTimeout(r, flipAtMs));
  const res = await fetch(`${api}/promotions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({
    name: 'Flash sale demo', discountType: 'percentage', value: '50',
    startsAt: new Date(Date.now() - 1000).toISOString(), endsAt: new Date(Date.now() + 3_600_000).toISOString(), target: { categoryId },
  }) });
  const promo = await res.json();
  flipped = true;
  console.log(`flash sale created at ${((Date.now() - started) / 1000).toFixed(1)}s -> promotion ${promo.id}`);
  return promo.id as string;
})();

await Promise.all(Array.from({ length: concurrency }, worker));
const promoId = await flip;
await fetch(`${api}/promotions/${promoId}/cancel`, { method: 'POST' });

const pct = (xs: number[], p: number) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))]!.toFixed(1) : 'n/a'; };
const report = (label: string, xs: number[], secs: number) =>
  console.log(`${label}: ${xs.length} req, ${(xs.length / secs).toFixed(0)} req/s, p50 ${pct(xs, 0.5)}ms, p95 ${pct(xs, 0.95)}ms, p99 ${pct(xs, 0.99)}ms`);
report('before flip', before, flipAtMs / 1000);
report('after flip ', after, (durationMs - flipAtMs) / 1000);
console.log(`errors: ${errors}; first item price before ${priceBefore} -> after ${priceAfter} (promotion cancelled again)`);
```

- [ ] **Step 5: Run the demos**

Run, with `pnpm dev:api` and `pnpm dev:runner` running in other terminals:
```bash
pnpm seed && pnpm vendor-file --rows=500000 && pnpm demo:ingest tmp/vendor-500k.csv
```
Expected: the job reaches `completed` with `rowsProcessed + rowsRejected = 500000`, roughly 10 to 12 chunks at the default chunk size, and no chunk failing. Record the elapsed time and rows per second for the ADR.

Run: `pnpm demo:flash-sale accessories`
Expected: the price of the first item halves after the flip, error count 0, and p95 after the flip within the same order of magnitude as before. Record both lines for the ADR.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: seed, vendor file generator, ingestion and flash sale demo scripts

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 17: README, ADR, AI appendix

**Files:**
- Create: `README.md`, `ADR.md`, `AI_APPENDIX.md`
- Modify: `schema.sql` (regenerate with `pnpm schema:export` if migrations changed)

**Interfaces:**
- Consumes: the demo output numbers recorded in Task 16.

- [ ] **Step 1: README**

`README.md` must contain these sections, in this order, with real commands:

1. **What this is**: two paragraphs. The ModaCo promotion API and the two scenario answers in one sentence each: read-time effective price with version-validated Redis cache; byte-range fan-out ingestion on Lambda.
2. **Quick start**:
   ```bash
   pnpm install
   docker compose up -d postgres redis localstack
   pnpm db:migrate
   pnpm seed
   pnpm dev:api        # terminal 1
   pnpm dev:runner     # terminal 2 (local Lambda stand-in)
   ```
   plus the all-in-Docker alternative `docker compose --profile app up --build`.
3. **Endpoints**: a table with method, path, purpose for every route in the spec, and one `curl` example each for list, detail, create promotion, cancel, assign, create ingestion job, job status.
4. **Demos**: the exact commands from Task 16 and what to look for in the output.
5. **Tests**: `pnpm test`, note that integration tests need the compose infrastructure, `pnpm test:unit` for the pure tests.
6. **Deploying the ingestion pipeline to AWS**: `pnpm build:lambda`, then `sam deploy --guided --template infra/template.yaml`, listing the two parameters and noting RDS Proxy for connection pooling in production.
7. **Layout**: the directory tree from this plan's File Structure with one line per top-level entry.
8. Pointers to `ADR.md`, `AI_APPENDIX.md`, and `schema.sql`.

- [ ] **Step 2: ADR**

`ADR.md` structure and content. Write it in full prose; each decision has Context, Decision, Consequences (trade-offs). Include the measured numbers from Task 16 in sections 5 and 6.

```
# Architecture Decision Record: ModaCo Promotion Management API

## 1. Context
  Domain summary, the two scenarios, the constraints (Node/Express/TS, serverless consumption plan for ingestion).

## 2. Stack: PostgreSQL + Drizzle, Redis, AWS Lambda/S3/SQS, LocalStack
  Why Postgres (numeric money, check constraints, lateral joins, partial indexes), why Drizzle (SQL stays visible;
  the effective-price query is one SQL fragment used everywhere), why Redis (versions + cache + counters),
  why LocalStack (reviewer can run everything with one command).

## 3. Effective price is computed at read time, never stored
  Context: a category promotion touches 50k+ products at once; a product created mid-sale must inherit it.
  Decision: lateral subquery selects the active promotion (direct or via category), most recent wins; CASE computes price.
  Consequences: creating a promotion is one row insert (O(1)); new products qualify automatically; reads need a join and
  effective-price sort cannot use an index. Mitigated by the cache (section 4).
  Rejected: materialized effective_price column (50k-row update inside a transaction, trigger for new products, two sources of truth).

## 4. Cache with version counters validated on read
  Decision: Redis entries carry the product/category version numbers they were built under; readers compare against
  live counters; a promotion write does one INCR after commit. Stock is excluded from the entry and read from a
  counter. TTL is capped at the next promotion boundary so scheduled starts/ends need no scheduler. Rebuilds are
  coalesced with a short lock; Redis failure degrades to Postgres.
  Consequences: instant invalidation of any number of entries with one increment; one extra MGET per read;
  ~200 ms stale window during a coalesced rebuild; 5-minute self-heal bound if a bump is lost.
  Rejected: delete-by-pattern (SCAN over 50k keys), version-stamped keys (needs the category before reading the product).

## 5. Scenario B walkthrough and measurement
  Flow of POST /promotions during load, what happens to the list pages, product-created-mid-sale path.
  Numbers from `pnpm demo:flash-sale` (before/after req/s, p50/p95/p99).

## 6. Scenario A: byte-range fan-out on Lambda
  Decision: presigned upload; splitter reads only Content-Length and enqueues arithmetic byte ranges; workers stream
  one range, own lines by first-byte position (read one byte early, over-read 64 KB), price rows in the shared core
  package, upsert in batches of 1000 with ON CONFLICT (sku), dedupe SKUs within a batch, bump versions after each
  commit. Chunk completion + job counters in one transaction; redelivery of a completed chunk is a no-op; DLQ after
  3 receives fails the job. Local runner spawns a child process per invocation with SIGKILL timeout and heap cap.
  Consequences: splitter time independent of file size; memory bounded by one batch; parallelism = reserved
  concurrency; connections bounded (RDS Proxy in production). Costs: line-alignment logic, one HEAD + one range GET
  per chunk, per-invocation cold start. Numbers from `pnpm demo:ingest` (rows/s, chunks, elapsed).
  Rejected: streaming splitter that rewrites chunk files (scales with file size, hits the timeout); single
  self-continuing Lambda with checkpoints (sequential, retry/checkpoint interplay).

## 7. Promotion conflict rule: most recent wins
  Decision and why it was chosen over precedence + exclusion constraints for this case study; what it means for
  a product promotion shadowed by a later category sale; how it stays consistent because it is evaluated at read time.

## 8. Pricing rules as an application-layer pipeline
  validate -> margin -> .99 -> clamp; category defaults; why it lives in packages/core and is imported by the worker.

## 9. Operational notes and what is out of scope
  No auth (internal API, gateway assumed); no rate limiting; product deletion; multi-currency; promotion stacking;
  editable rules via API. What would change first at 10x scale (read replicas, per-category list cache warming,
  RDS Proxy, S3 multipart + larger chunks, worker batch size >1 with partial batch responses).
```

- [ ] **Step 3: AI appendix**

`AI_APPENDIX.md` follows Form 5's four sections exactly. Draft it from this project's actual history; the candidate then edits the wording, ratio, and reflection so it is their own account.

```
# Form 5: AI Interaction Summary

## 1. Tool Manifest
| Model / Tool | Primary purpose | Effectiveness (1-5) and why |
| Claude Code (desktop app) running Claude Fable 5.1 | Requirements walkthrough, architecture brainstorming, design spec, implementation plan, code generation under TDD | (candidate's rating) |
| Superpowers plugin skills (brainstorming, writing-plans, test-driven-development) | Forced a question-by-question design phase and a test-first implementation order | (candidate's rating) |
| Context7 MCP (library docs) | Verified Drizzle ORM check/partial-index/upsert syntax and LocalStack S3->SQS notification setup before writing code | (candidate's rating) |

## 2. AI Tool Usage Approach
| Phase | Prompting strategy and context provided | Human refinement |
| Requirements | Asked the AI to read the case study PDF and the Form 5 template first and summarize before designing anything | Confirmed the summary, then answered one design question at a time |
| Architecture | Chose between 2-3 options per decision (serverless target, local-first with LocalStack, Postgres+Drizzle, read-time price + Redis, byte-range fan-out, conflict rule, pricing pipeline) | Overrode the AI's recommendation on the conflict rule (chose most-recent-wins for scope), pushed back twice on the cache design (below) |
| Spec and plan | Had the AI write a design spec, self-review it, then a task-by-task plan with tests written before code | Reviewed and approved each section; the plan is the artifact that was executed |
| Implementation | Executed the plan task by task with fresh context per task and a test run gating each commit | Reviewed diffs between tasks; corrected deviations from the spec |

Two most critical prompts:
1. "compute at read time with redis (but make sure that we can actually cache it, products have stock quantity can we safely cache that?)"
2. "what happens when we release a promo, unit prices change on the backend but cached responses will return old prices. Shall we add an invalidation logic or something else?"

## 3. Judgement, Challenges and Verification
| Challenge | Judgement / verification | Resolution |
| The AI's first cache design cached the whole product row, including stock, under a long TTL | Stock changes on every sale; a cached stock value would be wrong for the TTL window | Split the cache: catalog+price entry with a long TTL, stock read live from a Redis counter with Postgres fallback |
| The AI's spec keyed the product cache as product:{id}:v{pv}:c{cv} | The API cannot know a product's category (hence its category version) before reading the product, so the key could never be built on a cold read | Store the versions inside the cached value and validate them on read; same invalidation semantics, two round trips warm |
| The AI's first chunk-alignment rule was "discard everything up to the first newline when byteStart > 0" | Walked through the case where a chunk boundary falls exactly after a newline: the rule discards a full, owned line and no chunk processes it | Own a line by the position of its first byte; read one byte early to see the preceding character; property test over many chunk sizes asserts every line lands exactly once |
| Promotions with future start dates change prices with no write to trigger invalidation | A scheduler adds an operational component and a race | Cap each entry's TTL at the next promotion boundary computed from the promotions table |
| Multi-row upsert with duplicate SKUs in one batch | Postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second time" | Dedupe by SKU within a batch, last occurrence wins; covered by a test |

## 4. Overall Reflection
Estimated ratio: (candidate's estimate, e.g. "~65% AI-generated code and prose / ~35% human-directed design decisions and corrections")
Key takeaway: (candidate's own words; suggested angle: the AI produced confident designs that were structurally plausible but had gaps at exactly the boundaries the case study grades, namely cache correctness under mutation and chunk boundaries, and those gaps were only found by asking "what happens when..." questions rather than by reading the output.)
```

- [ ] **Step 4: Regenerate DDL and run everything once more**

Run: `pnpm schema:export && pnpm test && pnpm typecheck`
Expected: `schema.sql` unchanged or updated, all tests PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: README, architecture decision record, AI usage appendix

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review notes

- Spec coverage: data model (Task 2), pricing rules (3), effective price and next boundary (4), cache keys, TTL, versions, read-through (5), chunking with corrected line ownership (6), API skeleton and error envelope (7), product reads with cache and live stock (8), product create inheriting a category sale and stock patch (9), promotions create/cancel/assign with version bumps (10), presigned upload and job status (11), splitter (12), worker with batching, dedupe, publish (13), DLQ, Lambda adapters, runner with timeout and heap cap, SAM (14), end-to-end through S3 notification (15), demo scripts producing the ADR numbers (16), docs (17).
- Deviation from the spec, deliberate: the DLQ path is exercised by calling the handler directly in tests because the queue's visibility timeout is 360 s; the redrive policy itself is configured in both LocalStack and SAM.
- Deviation from the spec, deliberate: `ingestion_rejections.line_number` is the line's ordinal within its chunk, since a chunk cannot know absolute line numbers without reading the preceding chunks; the chunk index is stored alongside it.
- Type consistency checked: `ChunkMessage` is defined once in `splitter.ts` and imported by the worker, DLQ, and adapters; `IngestDeps` picks are used consistently; `ProductRecord` has no stock and `ProductItem` adds it.
