import express from 'express';
import { hostname } from 'node:os';
import request from 'supertest';
import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config';
import { instanceId } from '../src/middleware/instance-id';

describe('instance id', () => {
  it('sets X-Instance-Id on every response, including 404s', async () => {
    const app = express().use(instanceId('replica-7'));
    app.get('/ping', (_req, res) => { res.json({ ok: true }); });
    const ok = await request(app).get('/ping');
    expect(ok.headers['x-instance-id']).toBe('replica-7');
    const missing = await request(app).get('/nope');
    expect(missing.status).toBe(404);
    expect(missing.headers['x-instance-id']).toBe('replica-7');
  });

  it('reads INSTANCE_ID and falls back to the hostname', () => {
    expect(loadConfig({ INSTANCE_ID: 'api-1' }).instanceId).toBe('api-1');
    expect(loadConfig({}).instanceId).toBe(hostname());
  });

  it('treats an empty INSTANCE_ID as unset instead of failing config validation', () => {
    expect(loadConfig({ INSTANCE_ID: '' }).instanceId).toBe(hostname());
  });
});
