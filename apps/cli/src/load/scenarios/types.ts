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
  /**
   * How long a load-test promotion stays live after it is created. `load` passes warmup + duration + 60 s: long enough
   * to outlast the run, short enough that a run killed before its cleanup (a second Ctrl-C) leaves no discount behind
   * for long. Default 1 h.
   */
  promotionTtlMs?: number;
}

export const DEFAULT_PROMOTION_TTL_MS = 3_600_000;

/** What runLoad hands a scenario that orchestrates its own phases (flash-sale). */
export interface PhaseRunner {
  readonly client: ApiClient;
  /** The --duration budget for all recorded phases together. */
  readonly durationMs: number;
  log(msg: string): void;
  /** True once the run was interrupted (SIGINT). A scenario must then stop starting new work, writes above all. */
  aborted(): boolean;
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
