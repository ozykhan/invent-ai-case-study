import express, { type Express } from 'express';
import type { AppDeps } from './deps';
import { ingestionRoutes } from './ingestion/routes';
import { errorHandler } from './middleware/error-handler';
import { requestId } from './middleware/request-id';
import { productRoutes } from './products/routes';
import { promotionRoutes } from './promotions/routes';
import { healthRoutes } from './routes/health';

export function createApp(deps: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.use(requestId);
  app.use(express.json({ limit: '1mb' }));
  app.use(healthRoutes(deps));
  app.use(productRoutes(deps));
  app.use(promotionRoutes(deps));
  app.use(ingestionRoutes(deps));
  app.use((_req, res) => res.status(404).json({ error: { code: 'not_found', message: 'route not found' } }));
  app.use(errorHandler(deps.logger));
  return app;
}
