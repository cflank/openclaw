import fs from "node:fs";
import path from "node:path";

function pathExists(p: string): boolean {
  try {
    fs.accessSync(p, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

export function resolveOpenclawRoot(cwd = process.cwd()): string {
  const direct = path.resolve(cwd);
  const directPkg = path.join(direct, "package.json");
  if (pathExists(directPkg) && path.basename(direct).toLowerCase() === "openclaw") {
    return direct;
  }

  const nested = path.join(direct, "openclaw");
  const nestedPkg = path.join(nested, "package.json");
  if (pathExists(nestedPkg)) {
    return nested;
  }

  return direct;
}

export function resolveWorkspaceRoot(cwd = process.cwd()): string {
  const openclawRoot = resolveOpenclawRoot(cwd);
  if (path.basename(openclawRoot).toLowerCase() === "openclaw") {
    return path.dirname(openclawRoot);
  }
  return openclawRoot;
}

export function resolveTacPolicyPath(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_POLICY_PATH) {
    return process.env.TAC_POLICY_PATH;
  }
  return path.join(resolveOpenclawRoot(), "TAC", "config", "policy", "owner-tool-skill-policy.json");
}

export function resolveTacFallbackPath(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_FALLBACK_PATH) {
    return process.env.TAC_FALLBACK_PATH;
  }
  return path.join(resolveOpenclawRoot(), "TAC", "config", "policy", "fallback-policy.json");
}

export function resolveTacGateThresholdPath(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_GATE_THRESHOLDS_PATH) {
    return process.env.TAC_GATE_THRESHOLDS_PATH;
  }
  return path.join(resolveOpenclawRoot(), "TAC", "config", "policy", "gate-thresholds.json");
}

export function resolveRiskVerdictSchemaPath(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_RISK_VERDICT_SCHEMA_PATH) {
    return process.env.TAC_RISK_VERDICT_SCHEMA_PATH;
  }
  return path.join(resolveOpenclawRoot(), "extensions", "TAC", "schemas", "risk_verdict.schema.json");
}

export function resolveTacContractsDir(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_CONTRACTS_DIR) {
    return process.env.TAC_CONTRACTS_DIR;
  }
  return path.join(resolveWorkspaceRoot(), "runtime", "TAC", "contracts");
}

export function resolveTacAuditDir(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_AUDIT_DIR) {
    return process.env.TAC_AUDIT_DIR;
  }
  return path.join(resolveWorkspaceRoot(), "runtime", "TAC", "audit");
}

export function resolveTacCheckpointDir(customPath?: string): string {
  if (customPath) {
    return customPath;
  }
  if (process.env.TAC_CHECKPOINT_DIR) {
    return process.env.TAC_CHECKPOINT_DIR;
  }
  return path.join(resolveWorkspaceRoot(), "runtime", "TAC", "checkpoints");
}

