import fs from "node:fs";
import path from "node:path";
import {
  resolveTacAuditDir,
  resolveTacCheckpointDir,
  resolveTacContractsDir,
} from "../shared/paths.js";

export interface RecoveryLoadInput {
  sessionId: string;
  checkpointDir?: string;
  contractsDir?: string;
  auditDir?: string;
}

export interface RecoveryLoadResult {
  recovered: boolean;
  next_stage: string | null;
  reason_code: string;
  needs_manual_reconcile: boolean;
  checkpoint: Record<string, unknown> | null;
  latest_contract: Record<string, unknown> | null;
  latest_audit_event: Record<string, unknown> | null;
}

function readJsonIfExists(filePath: string): Record<string, unknown> | null {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function collectJsonFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) {
    return [];
  }
  const out: string[] = [];
  const stack = [dir];
  while (stack.length > 0) {
    const cur = stack.pop()!;
    for (const ent of fs.readdirSync(cur, { withFileTypes: true })) {
      const p = path.join(cur, ent.name);
      if (ent.isDirectory()) {
        stack.push(p);
      } else if (ent.isFile() && p.endsWith(".json")) {
        out.push(p);
      }
    }
  }
  return out;
}

function readLatestContract(contractsDir: string, sessionId: string): Record<string, unknown> | null {
  const sessionDir = path.join(contractsDir, sessionId);
  const files = collectJsonFiles(sessionDir);
  if (files.length === 0) {
    return null;
  }
  files.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return readJsonIfExists(files[0]!);
}

function readLatestAuditEvent(auditDir: string, sessionId: string): Record<string, unknown> | null {
  if (!fs.existsSync(auditDir)) {
    return null;
  }
  const files = fs
    .readdirSync(auditDir)
    .filter((f) => f.endsWith(".jsonl"))
    .map((f) => path.join(auditDir, f))
    .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  for (const fp of files) {
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const line = lines[i]!;
      try {
        const parsed = JSON.parse(line) as Record<string, unknown>;
        if (parsed.session_id === sessionId) {
          return parsed;
        }
      } catch {
        // Skip malformed line.
      }
    }
  }
  return null;
}

export function loadRecoveryState(input: RecoveryLoadInput): RecoveryLoadResult {
  const checkpointDir = resolveTacCheckpointDir(input.checkpointDir);
  const contractsDir = resolveTacContractsDir(input.contractsDir);
  const auditDir = resolveTacAuditDir(input.auditDir);

  const checkpoint = readJsonIfExists(path.join(checkpointDir, `${input.sessionId}.json`));
  const latestContract = readLatestContract(contractsDir, input.sessionId);
  const latestAuditEvent = readLatestAuditEvent(auditDir, input.sessionId);

  const nextStage =
    (checkpoint?.current_stage as string | undefined) ??
    (latestContract?.stage as string | undefined) ??
    null;

  const recovered = Boolean(checkpoint || latestContract || latestAuditEvent);

  return {
    recovered,
    next_stage: nextStage,
    reason_code: recovered ? "recovered_from_runtime_state" : "no_runtime_state_found",
    needs_manual_reconcile: !recovered,
    checkpoint,
    latest_contract: latestContract,
    latest_audit_event: latestAuditEvent,
  };
}
