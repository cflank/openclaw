import fs from "node:fs";
import { resolveTacFallbackPath } from "../shared/paths.js";

export type MarketDomain = "CN" | "HK" | "US" | "US_FINANCIALS" | "CRYPTO";

interface FallbackPolicy {
  markets: Record<MarketDomain, string[]>;
  events: {
    step_used: string;
    exhausted: string;
  };
}

export interface FallbackExecutionInput {
  market: MarketDomain;
  // Simulated availability; omitted source defaults to available=true.
  sourceAvailability?: Record<string, boolean>;
}

export interface FallbackExecutionOptions {
  policyPath?: string;
  policy?: FallbackPolicy;
}

export interface FallbackExecutionResult {
  market: MarketDomain;
  step_used: string[];
  exhausted: boolean;
  result_source: string | null;
  events: string[];
}

let cachedPath: string | null = null;
let cachedPolicy: FallbackPolicy | null = null;

function loadPolicy(pathOverride?: string): FallbackPolicy {
  const p = resolveTacFallbackPath(pathOverride);
  if (cachedPolicy && cachedPath === p) {
    return cachedPolicy;
  }
  const parsed = JSON.parse(fs.readFileSync(p, "utf8")) as FallbackPolicy;
  cachedPath = p;
  cachedPolicy = parsed;
  return parsed;
}

export function executeFallback(
  input: FallbackExecutionInput,
  options: FallbackExecutionOptions = {},
): FallbackExecutionResult {
  const policy = options.policy ?? loadPolicy(options.policyPath);
  const chain = policy.markets[input.market] ?? [];
  const stepUsed: string[] = [];
  const events: string[] = [];

  if (chain.length === 0) {
    return {
      market: input.market,
      step_used: [],
      exhausted: true,
      result_source: null,
      events: [policy.events.exhausted],
    };
  }

  for (const source of chain) {
    stepUsed.push(source);
    events.push(policy.events.step_used);
    const available = input.sourceAvailability?.[source] ?? true;
    if (available) {
      return {
        market: input.market,
        step_used: stepUsed,
        exhausted: false,
        result_source: source,
        events,
      };
    }
  }

  events.push(policy.events.exhausted);
  return {
    market: input.market,
    step_used: stepUsed,
    exhausted: true,
    result_source: null,
    events,
  };
}
