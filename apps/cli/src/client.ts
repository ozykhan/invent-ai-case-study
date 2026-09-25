import { Agent, request } from 'undici';
import type {
  CreateProductInput, CreatePromotionInput, HealthBody, IngestionJob, IngestionJobCreated, ListProductsQuery,
  Page, Product, Promotion, Rejection, StockInput, Target,
} from './api-types';
import { ApiError } from './errors';
import type { RequestSpec, SendResult, Transport } from './load/engine';

type Method = RequestSpec['method'];

export interface Reply<T> { status: number; body: T; instance?: string }

export interface ClientOptions {
  baseUrl: string;
  timeoutMs: number;
  /** Connection pool size to the target. Default 16, which is plenty for one-off commands. */
  connections?: number;
}

/** Bodies up to this size are read to the end so the keep-alive connection can be reused. */
const MAX_DRAIN_BYTES = 8 * 1024 * 1024;

const headerValue = (v: string | string[] | undefined): string | undefined => (Array.isArray(v) ? v[0] : v);

function parseJson(text: string): unknown {
  if (!text) return undefined;
  try { return JSON.parse(text); } catch { return text; }
}

function query(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined) q.set(k, String(v));
  const s = q.toString();
  return s ? `?${s}` : '';
}

export class ApiClient implements Transport {
  readonly baseUrl: string;
  private readonly agent: Agent;
  private readonly timeoutMs: number;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.timeoutMs = opts.timeoutMs;
    this.agent = new Agent({ connections: opts.connections ?? 16, keepAliveTimeout: 30_000, connect: { timeout: opts.timeoutMs } });
  }

  private dispatch(method: Method, path: string, body?: unknown) {
    return request(`${this.baseUrl}${path}`, {
      dispatcher: this.agent,
      method,
      headers: body === undefined ? {} : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      headersTimeout: this.timeoutMs,
      bodyTimeout: this.timeoutMs,
    });
  }

  /** Load path: never throws on status; drains the body unless the spec asks for it. */
  async send(spec: RequestSpec): Promise<SendResult> {
    const res = await this.dispatch(spec.method, spec.path, spec.body);
    const instance = headerValue(res.headers['x-instance-id']);
    if (spec.onResponse) return { status: res.statusCode, instance, body: parseJson(await res.body.text()) };
    await res.body.dump({ limit: MAX_DRAIN_BYTES });
    return { status: res.statusCode, instance };
  }

  /** Operational path: parses JSON and throws ApiError on 4xx/5xx unless the status is in `accept`. */
  async call<T>(method: Method, path: string, body?: unknown, accept: number[] = []): Promise<Reply<T>> {
    const res = await this.dispatch(method, path, body);
    const text = await res.body.text();
    const parsed = parseJson(text);
    const instance = headerValue(res.headers['x-instance-id']);
    if (res.statusCode >= 400 && !accept.includes(res.statusCode)) {
      const e = (parsed as { error?: { code?: string; message?: string; details?: unknown } } | undefined)?.error;
      throw new ApiError(res.statusCode, e?.code ?? `http_${res.statusCode}`, e?.message ?? (text.slice(0, 200) || `HTTP ${res.statusCode}`), e?.details);
    }
    return { status: res.statusCode, body: parsed as T, instance };
  }

  health(): Promise<Reply<HealthBody>> {
    return this.call<HealthBody>('GET', '/health', undefined, [503]);
  }

  async listProducts(q: ListProductsQuery = {}): Promise<Page<Product>> {
    return (await this.call<Page<Product>>('GET', `/products${query({ category: q.category, sort: q.sort, page: q.page, pageSize: q.pageSize })}`)).body;
  }

  async getProduct(id: number): Promise<Product> {
    return (await this.call<Product>('GET', `/products/${id}`)).body;
  }

  async createProduct(input: CreateProductInput): Promise<Product> {
    return (await this.call<Product>('POST', '/products', input)).body;
  }

  async setStock(id: number, input: StockInput): Promise<{ id: number; stock: number }> {
    return (await this.call<{ id: number; stock: number }>('PATCH', `/products/${id}/stock`, input)).body;
  }

  async createPromotion(input: CreatePromotionInput): Promise<Promotion> {
    return (await this.call<Promotion>('POST', '/promotions', input)).body;
  }

  async getPromotion(id: string): Promise<Promotion> {
    return (await this.call<Promotion>('GET', `/promotions/${encodeURIComponent(id)}`)).body;
  }

  async cancelPromotion(id: string): Promise<Promotion> {
    return (await this.call<Promotion>('POST', `/promotions/${encodeURIComponent(id)}/cancel`)).body;
  }

  async retargetPromotion(id: string, target: Target): Promise<Promotion> {
    return (await this.call<Promotion>('PUT', `/promotions/${encodeURIComponent(id)}/target`, target)).body;
  }

  async createIngestionJob(filename: string): Promise<IngestionJobCreated> {
    return (await this.call<IngestionJobCreated>('POST', '/ingestion/jobs', { filename })).body;
  }

  async getIngestionJob(id: string): Promise<IngestionJob> {
    return (await this.call<IngestionJob>('GET', `/ingestion/jobs/${encodeURIComponent(id)}`)).body;
  }

  async listRejections(id: string, q: { page?: number; pageSize?: number } = {}): Promise<Page<Rejection>> {
    return (await this.call<Page<Rejection>>('GET', `/ingestion/jobs/${encodeURIComponent(id)}/rejections${query({ page: q.page, pageSize: q.pageSize })}`)).body;
  }

  /**
   * Closes the connection pool. By default it waits for requests still in flight (each bounded by --timeout).
   * `force` aborts them instead: an interrupted load run must exit within its drain window.
   */
  close(opts: { force?: boolean } = {}): Promise<void> {
    return opts.force ? this.agent.destroy() : this.agent.close();
  }
}
