import { createApp } from './app';
import { loadConfig } from './config';
import { createDeps } from './deps';

const config = loadConfig();
const deps = await createDeps(config);
const app = createApp(deps);
const server = app.listen(config.port, () => deps.logger.info({ port: config.port }, 'api listening'));

const shutdown = async () => {
  server.close();
  await deps.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
