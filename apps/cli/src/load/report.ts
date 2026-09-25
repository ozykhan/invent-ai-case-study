import { formatTable } from '../output';
import type { ProgressSample } from './engine';
import type { StatsSummary } from './metrics';

export interface Check { name: string; ok: boolean; message: string }

export interface PhaseResult { name: string; elapsedSeconds: number; total: StatsSummary; byLabel: Record<string, StatsSummary> }

/** What `load --json` prints and `--out` writes. */
export interface ResultDocument {
  scenario: string;
  target: string;
  startedAt: string;
  options: Record<string, unknown>;
  phases: PhaseResult[];
  /** Responses per X-Instance-Id over all recorded phases. */
  instances: Record<string, number>;
  checks: Check[];
  notes: Record<string, string>;
  interrupted: boolean;
  /** False when any check failed. */
  ok: boolean;
}

export function formatProgress(phase: string, s: ProgressSample): string {
  return `[${phase} ${s.elapsedSeconds.toFixed(1)}s] ${s.rps.toFixed(0)} req/s  p50 ${s.p50Ms}ms  p99 ${s.p99Ms}ms  errors ${s.errors}  dropped ${s.dropped}  in-flight ${s.inflight}`;
}

const HEADER = ['label', 'count', 'req/s', 'p50', 'p90', 'p95', 'p99', 'p99.9', 'max', 'errors', 'dropped', 'status'];
const errorCount = (s: StatsSummary) => Object.values(s.errors).reduce((sum, n) => sum + (n ?? 0), 0);
const pairs = (o: Record<string, unknown>) => Object.entries(o).map(([k, v]) => `${k}:${String(v)}`).join(' ');
const statsRow = (label: string, s: StatsSummary) => [
  label, s.count, s.rps, s.latencyMs.p50, s.latencyMs.p90, s.latencyMs.p95, s.latencyMs.p99, s.latencyMs.p999, s.latencyMs.max,
  errorCount(s), s.dropped, pairs(s.status) || '-',
];
const indent = (text: string) => text.split('\n').map((line) => `  ${line}`).join('\n');
const describeOptions = (o: Record<string, unknown>) =>
  Object.entries(o).map(([k, v]) => `${k}=${typeof v === 'object' && v !== null ? JSON.stringify(v) : String(v)}`).join(' ');

export function formatReport(doc: ResultDocument): string {
  const lines = [`${doc.scenario} -> ${doc.target}  ${describeOptions(doc.options)}${doc.interrupted ? '  (INTERRUPTED)' : ''}`];
  for (const p of doc.phases) {
    lines.push('', `phase ${p.name} (${p.elapsedSeconds}s), latency in ms`);
    lines.push(indent(formatTable([HEADER, statsRow('total', p.total), ...Object.entries(p.byLabel).map(([label, s]) => statsRow(label, s))])));
    const errors = pairs(p.total.errors);
    if (errors) lines.push(`  errors: ${errors}`);
    if (p.total.dropped > 0) {
      lines.push(`  warning: ${p.total.dropped} requests dropped at --max-inflight; the target or this client could not keep up with the rate`);
    }
  }
  const served = Object.values(doc.instances).reduce((a, b) => a + b, 0);
  if (served > 0) {
    lines.push('', `instances (${Object.keys(doc.instances).length})`);
    lines.push(indent(formatTable(Object.entries(doc.instances).map(([id, n]) => [id, n, `${((n / served) * 100).toFixed(1)}%`]))));
  }
  if (doc.checks.length > 0) {
    lines.push('', 'checks');
    for (const c of doc.checks) lines.push(`  ${c.ok ? 'PASS' : 'FAIL'} ${c.name}: ${c.message}`);
  }
  const notes = Object.entries(doc.notes);
  if (notes.length > 0) lines.push('', 'notes', indent(formatTable(notes)));
  return lines.join('\n');
}
