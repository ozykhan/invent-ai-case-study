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
  uniqueIndex('ingestion_rejections_job_chunk_line_uq').on(t.jobId, t.chunkIndex, t.lineNumber),
]);
