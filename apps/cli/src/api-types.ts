/** Response and input shapes of the ModaCo API, as the CLI uses them. Money is a decimal string. */

export interface Category { id: number; name: string; slug: string }

export interface ActivePromotion { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string }

export interface Product {
  id: number;
  sku: string;
  name: string;
  category: Category;
  basePrice: string;
  effectivePrice: string;
  activePromotion: ActivePromotion | null;
  stock: number;
}

export interface Page<T> { items: T[]; pagination: { page: number; pageSize: number; total: number } }

export interface ListProductsQuery { category?: string; sort?: 'effective_price' | '-effective_price'; page?: number; pageSize?: number }

export interface CreateProductInput { sku: string; name: string; categoryId: number; basePrice: string; stock?: number }

export type StockInput = { stock: number } | { delta: number };

export type Target = { productId: number } | { categoryId: number };

export interface CreatePromotionInput {
  name: string;
  discountType: 'percentage' | 'fixed';
  value: string;
  startsAt: string;
  endsAt: string;
  target: Target;
}

export interface Promotion {
  id: string;
  name: string;
  discountType: 'percentage' | 'fixed';
  value: string;
  startsAt: string;
  endsAt: string;
  target: Target;
  cancelledAt: string | null;
  createdAt: string;
}

export interface IngestionJobCreated { jobId: string; uploadUrl: string; key: string; expiresInSeconds: number }

export interface IngestionJob {
  id: string;
  status: 'pending' | 'splitting' | 'processing' | 'completed' | 'failed';
  s3Key: string;
  totalChunks: number;
  completedChunks: number;
  failedChunks: number;
  rowsProcessed: number;
  rowsRejected: number;
  error: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Rejection { chunkIndex: number; lineNumber: number; rawLine: string; reason: string }

export interface HealthBody { status: 'ok' | 'degraded'; checks: { postgres: boolean; redis: boolean } }
