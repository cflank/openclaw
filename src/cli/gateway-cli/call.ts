import type { Command } from "commander";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { callGateway } from "../../gateway/call.js";
import { isOperatorScope, type OperatorScope } from "../../gateway/operator-scopes.js";
import { GATEWAY_CLIENT_MODES, GATEWAY_CLIENT_NAMES } from "../../gateway/protocol/client-info.js";
import { withProgress } from "../progress.js";

export type GatewayRpcOpts = {
  config?: OpenClawConfig;
  url?: string;
  token?: string;
  password?: string;
  timeout?: string;
  expectFinal?: boolean;
  json?: boolean;
  scope?: string[];
};

function collectScope(value: string, previous: string[] = []) {
  const trimmed = value.trim();
  return trimmed ? [...previous, trimmed] : previous;
}

function normalizeScopeOption(scope: string[] | undefined): OperatorScope[] | undefined {
  if (!Array.isArray(scope) || scope.length === 0) {
    return undefined;
  }
  const normalized: OperatorScope[] = [];
  for (const entry of scope) {
    if (!isOperatorScope(entry)) {
      throw new Error(`Unknown gateway operator scope: ${entry}`);
    }
    normalized.push(entry);
  }
  return normalized;
}

export const gatewayCallOpts = (cmd: Command) =>
  cmd
    .option("--url <url>", "Gateway WebSocket URL (defaults to gateway.remote.url when configured)")
    .option("--token <token>", "Gateway token (if required)")
    .option("--password <password>", "Gateway password (password auth)")
    .option("--timeout <ms>", "Timeout in ms", "10000")
    .option("--expect-final", "Wait for final response (agent)", false)
    .option("--json", "Output JSON", false)
    .option(
      "--scope <scope>",
      "Operator scope to request for this call (repeatable)",
      collectScope,
      [],
    );

export const callGatewayCli = async (method: string, opts: GatewayRpcOpts, params?: unknown) =>
  withProgress(
    {
      label: `Gateway ${method}`,
      indeterminate: true,
      enabled: opts.json !== true,
    },
    async () => {
      const scopes = normalizeScopeOption(opts.scope);
      return await callGateway({
        config: opts.config,
        url: opts.url,
        token: opts.token,
        password: opts.password,
        method,
        params,
        expectFinal: Boolean(opts.expectFinal),
        timeoutMs: Number(opts.timeout ?? 10_000),
        clientName: GATEWAY_CLIENT_NAMES.CLI,
        mode: GATEWAY_CLIENT_MODES.CLI,
        ...(scopes ? { scopes } : {}),
      });
    },
  );
