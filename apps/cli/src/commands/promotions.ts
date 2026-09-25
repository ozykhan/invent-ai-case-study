import { Option, type Command } from 'commander';
import type { Promotion, Target } from '../api-types';
import { UsageError } from '../errors';
import { emit, formatFields } from '../output';
import { intArg, withClient } from './common';

interface TargetFlags { product?: number; category?: number }

function targetOf(opts: TargetFlags): Target {
  if ((opts.product === undefined) === (opts.category === undefined)) throw new UsageError('pass exactly one of --product or --category');
  return opts.product !== undefined ? { productId: opts.product } : { categoryId: opts.category! };
}

const fields = (p: Promotion) => formatFields([
  ['id', p.id],
  ['name', p.name],
  ['discount', `${p.discountType} ${p.value}`],
  ['startsAt', p.startsAt],
  ['endsAt', p.endsAt],
  ['target', 'productId' in p.target ? `product ${p.target.productId}` : `category ${p.target.categoryId}`],
  ['cancelledAt', p.cancelledAt],
  ['createdAt', p.createdAt],
]);

export function registerPromotions(program: Command): void {
  const promotions = program.command('promotions').description('Create, read, cancel and retarget promotions');

  promotions.command('create')
    .description('POST /promotions')
    .requiredOption('--name <name>', 'promotion name')
    .addOption(new Option('--type <type>', 'discount type').choices(['percentage', 'fixed']).makeOptionMandatory())
    .requiredOption('--value <value>', 'percent (e.g. 50) or amount (e.g. 5.00), as a decimal string')
    .option('--starts <iso>', 'start time, ISO 8601 (default: now minus 1 s)')
    .option('--ends <iso>', 'end time, ISO 8601 (default: now plus 1 h)')
    .option('--product <id>', 'target one product', intArg('product'))
    .option('--category <id>', 'target a whole category', intArg('category'))
    .action((opts: TargetFlags & { name: string; type: 'percentage' | 'fixed'; value: string; starts?: string; ends?: string }, cmd: Command) => withClient(cmd, async (client, g) => {
      const target = targetOf(opts);
      const now = Date.now();
      const p = await client.createPromotion({
        name: opts.name, discountType: opts.type, value: opts.value,
        startsAt: opts.starts ?? new Date(now - 1000).toISOString(),
        endsAt: opts.ends ?? new Date(now + 3_600_000).toISOString(),
        target,
      });
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('get')
    .description('GET /promotions/:id')
    .argument('<id>', 'promotion id (uuid)')
    .action((id: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.getPromotion(id);
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('cancel')
    .description('POST /promotions/:id/cancel (idempotent)')
    .argument('<id>', 'promotion id (uuid)')
    .action((id: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.cancelPromotion(id);
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('target')
    .description('PUT /promotions/:id/target: move the promotion to another product or category')
    .argument('<id>', 'promotion id (uuid)')
    .option('--product <id>', 'new product target', intArg('product'))
    .option('--category <id>', 'new category target', intArg('category'))
    .action((id: string, opts: TargetFlags, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.retargetPromotion(id, targetOf(opts));
      emit(g.json, p, () => fields(p));
    }));
}
