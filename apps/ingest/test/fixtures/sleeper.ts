// Test fixture for runner.test.ts: writes its own real OS pid to `pidFile` immediately, then sleeps far
// longer than any test timeout before writing `doneFile`. A timeout test invokes this, waits, then checks
// that the pid recorded in `pidFile` is actually dead (not just some wrapper process) and that `doneFile`
// was never written.
import { writeFileSync } from 'node:fs';

const [, , pidFile, doneFile] = process.argv;
writeFileSync(pidFile!, String(process.pid));
await new Promise((resolve) => setTimeout(resolve, 5000));
writeFileSync(doneFile!, 'done');
