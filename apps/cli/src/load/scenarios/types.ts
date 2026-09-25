import type { ApiClient } from '../../client';
import type { LoadSource } from '../engine';
import type { Check } from '../report';

export interface ScenarioOptions {
  /** Restrict requests to one category slug. */
  category?: string;
  /** Highest listing page requested. */
  maxPage: number;
  /** Relative weights by request label; the scenario's default mix when absent. */
  mix?: Record<string, number>;
}

/** What runLoad hands a scenario that orchestrates its own phases (flash-sale). */
export interface PhaseRunner {
  readonly client: ApiClient;
  /** The --duration budget for all recorded phases together. */
  readonly durationMs: number;
  log(msg: string): void;
  /** Runs --warmup of unrecorded load. */
  warmup(source?: LoadSource): Promise<void>;
  /** Runs and records one phase. */
  phase(name: string, durationMs: number, source?: LoadSource): Promise<void>;
  addCheck(check: Check): void;
  note(key: string, value: string): void;
}

export interface Scenario extends LoadSource {
  readonly name: string;
  /** Discovers ids and categories before load starts. Not measured. */
  setup(client: ApiClient, log: (msg: string) => void): Promise<void>;
  /** Custom phases. Default: warmup, then one recorded phase named "main". */
  run?(runner: PhaseRunner): Promise<void>;
  /** Undoes the scenario's writes. Runs even when the run fails or is interrupted. */
  cleanup?(client: ApiClient): Promise<void>;
}
