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
