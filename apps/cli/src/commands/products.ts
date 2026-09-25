import { Option, type Command } from 'commander';
import type { ListProductsQuery, Product } from '../api-types';
import { UsageError } from '../errors';
import { emit, formatFields, formatTable } from '../output';
import { intArg, withClient } from './common';

const HEADER = ['id', 'sku', 'name', 'category', 'base', 'effective', 'promotion', 'stock'];
const row = (p: Product) => [p.id, p.sku, p.name, p.category.slug, p.basePrice, p.effectivePrice, p.activePromotion?.name ?? '-', p.stock];

const fields = (p: Product) => formatFields([
  ['id', p.id],
  ['sku', p.sku],
  ['name', p.name],
  ['category', `${p.category.slug} (id ${p.category.id})`],
  ['basePrice', p.basePrice],
  ['effectivePrice', p.effectivePrice],
  ['activePromotion', p.activePromotion ? `${p.activePromotion.name} (${p.activePromotion.discountType} ${p.activePromotion.value}, id ${p.activePromotion.id})` : null],
  ['stock', p.stock],
]);

export function registerProducts(program: Command): void {
  const products = program.command('products').description('List, read and create products, and change stock');

  products.command('list')
    .description('GET /products')
    .option('--category <slug>', 'category slug, e.g. accessories')
    .addOption(new Option('--sort <sort>', 'sort order (use --sort=-effective_price for descending)').choices(['effective_price', '-effective_price']))
    .option('--page <n>', 'page number (max 1000)', intArg('page'))
    .option('--page-size <n>', 'items per page (max 100)', intArg('page-size'))
    .action((opts: ListProductsQuery, cmd: Command) => withClient(cmd, async (client, g) => {
      const page = await client.listProducts(opts);
      emit(g.json, page, () => `${formatTable([HEADER, ...page.items.map(row)])}\npage ${page.pagination.page}, ${page.items.length} of ${page.pagination.total} products`);
    }));

  products.command('get')
    .description('GET /products/:id')
    .argument('<id>', 'product id', intArg('id'))
    .action((id: number, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.getProduct(id);
      emit(g.json, p, () => fields(p));
    }));

  products.command('create')
    .description('POST /products (inherits any active category promotion immediately)')
    .requiredOption('--sku <sku>', 'unique SKU')
    .requiredOption('--name <name>', 'display name')
    .requiredOption('--category-id <id>', 'category id', intArg('category-id'))
    .requiredOption('--base-price <price>', 'decimal string, e.g. 19.99')
    .option('--stock <n>', 'initial stock', intArg('stock', 0), 0)
    .action((opts: { sku: string; name: string; categoryId: number; basePrice: string; stock: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.createProduct(opts);
      emit(g.json, p, () => fields(p));
    }));

  products.command('stock')
    .description('PATCH /products/:id/stock: set with --set, or adjust with --delta')
    .argument('<id>', 'product id', intArg('id'))
    .option('--set <n>', 'set the stock to n', intArg('set', 0))
    .option('--delta <n>', 'add n (negative removes)', intArg('delta', -2_147_483_648))
    .action((id: number, opts: { set?: number; delta?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      if ((opts.set === undefined) === (opts.delta === undefined)) throw new UsageError('pass exactly one of --set or --delta');
      const r = await client.setStock(id, opts.set !== undefined ? { stock: opts.set } : { delta: opts.delta! });
      emit(g.json, r, () => `product ${r.id}: stock ${r.stock}`);
    }));
}
