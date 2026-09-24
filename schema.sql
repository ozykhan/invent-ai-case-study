-- ModaCo Promotion Management API: PostgreSQL DDL
-- Generated from packages/core/drizzle by scripts/export-schema.ts

-- 0000_red_susan_delgado.sql
CREATE TYPE "public"."ingestion_chunk_status" AS ENUM('pending', 'processing', 'completed', 'failed');
CREATE TYPE "public"."discount_type" AS ENUM('percentage', 'fixed');
CREATE TYPE "public"."ingestion_job_status" AS ENUM('pending', 'splitting', 'processing', 'completed', 'failed');
CREATE TYPE "public"."promotion_scope" AS ENUM('product', 'category');
CREATE TABLE "categories" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "categories_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"name" text NOT NULL,
	"slug" text NOT NULL,
	"margin_pct" numeric(6, 2) DEFAULT '30.00' NOT NULL,
	"price_floor" numeric(12, 2) DEFAULT '0.99' NOT NULL,
	"price_ceiling" numeric(12, 2) DEFAULT '99999.99' NOT NULL,
	CONSTRAINT "categories_name_unique" UNIQUE("name"),
	CONSTRAINT "categories_slug_unique" UNIQUE("slug")
);

CREATE TABLE "ingestion_chunks" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ingestion_chunks_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"job_id" uuid NOT NULL,
	"chunk_index" integer NOT NULL,
	"byte_start" bigint NOT NULL,
	"byte_end" bigint NOT NULL,
	"status" "ingestion_chunk_status" DEFAULT 'pending' NOT NULL,
	"attempts" integer DEFAULT 0 NOT NULL,
	"rows_processed" integer DEFAULT 0 NOT NULL,
	"rows_rejected" integer DEFAULT 0 NOT NULL,
	"error" text,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "ingestion_jobs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"s3_key" text NOT NULL,
	"status" "ingestion_job_status" DEFAULT 'pending' NOT NULL,
	"total_chunks" integer DEFAULT 0 NOT NULL,
	"completed_chunks" integer DEFAULT 0 NOT NULL,
	"failed_chunks" integer DEFAULT 0 NOT NULL,
	"rows_processed" integer DEFAULT 0 NOT NULL,
	"rows_rejected" integer DEFAULT 0 NOT NULL,
	"error" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "ingestion_rejections" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ingestion_rejections_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"job_id" uuid NOT NULL,
	"chunk_index" integer NOT NULL,
	"line_number" integer NOT NULL,
	"raw_line" text NOT NULL,
	"reason" text NOT NULL
);

CREATE TABLE "products" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "products_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"sku" text NOT NULL,
	"name" text NOT NULL,
	"category_id" integer NOT NULL,
	"base_price" numeric(12, 2) NOT NULL,
	"stock" integer DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "products_sku_unique" UNIQUE("sku"),
	CONSTRAINT "products_stock_nonnegative" CHECK ("products"."stock" >= 0),
	CONSTRAINT "products_base_price_nonnegative" CHECK ("products"."base_price" >= 0)
);

CREATE TABLE "promotions" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"name" text NOT NULL,
	"discount_type" "discount_type" NOT NULL,
	"value" numeric(12, 2) NOT NULL,
	"starts_at" timestamp with time zone NOT NULL,
	"ends_at" timestamp with time zone NOT NULL,
	"scope" "promotion_scope" NOT NULL,
	"product_id" integer,
	"category_id" integer,
	"cancelled_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "promotions_scope_target" CHECK (("promotions"."scope" = 'product' and "promotions"."product_id" is not null and "promotions"."category_id" is null) or ("promotions"."scope" = 'category' and "promotions"."category_id" is not null and "promotions"."product_id" is null)),
	CONSTRAINT "promotions_window" CHECK ("promotions"."ends_at" > "promotions"."starts_at"),
	CONSTRAINT "promotions_value" CHECK ("promotions"."value" >= 0 and ("promotions"."discount_type" <> 'percentage' or "promotions"."value" <= 100))
);

ALTER TABLE "ingestion_chunks" ADD CONSTRAINT "ingestion_chunks_job_id_ingestion_jobs_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ingestion_jobs"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "ingestion_rejections" ADD CONSTRAINT "ingestion_rejections_job_id_ingestion_jobs_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ingestion_jobs"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "products" ADD CONSTRAINT "products_category_id_categories_id_fk" FOREIGN KEY ("category_id") REFERENCES "public"."categories"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "promotions" ADD CONSTRAINT "promotions_product_id_products_id_fk" FOREIGN KEY ("product_id") REFERENCES "public"."products"("id") ON DELETE no action ON UPDATE no action;
ALTER TABLE "promotions" ADD CONSTRAINT "promotions_category_id_categories_id_fk" FOREIGN KEY ("category_id") REFERENCES "public"."categories"("id") ON DELETE no action ON UPDATE no action;
CREATE UNIQUE INDEX "ingestion_chunks_job_index_uq" ON "ingestion_chunks" USING btree ("job_id","chunk_index");
CREATE INDEX "ingestion_rejections_job_idx" ON "ingestion_rejections" USING btree ("job_id");
CREATE INDEX "products_category_base_price_idx" ON "products" USING btree ("category_id","base_price");
CREATE INDEX "promotions_category_window_idx" ON "promotions" USING btree ("category_id","starts_at","ends_at") WHERE "promotions"."cancelled_at" is null;
CREATE INDEX "promotions_product_window_idx" ON "promotions" USING btree ("product_id","starts_at","ends_at") WHERE "promotions"."cancelled_at" is null;