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
