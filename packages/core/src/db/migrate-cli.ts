import { createDb, TEST_DATABASE_URL } from './client';
import { runMigrations } from './migrate';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 1 });
await runMigrations(db);
await close();
console.log('migrations applied');
