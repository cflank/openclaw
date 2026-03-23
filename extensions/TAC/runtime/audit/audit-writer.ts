import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { resolveTacAuditDir } from "../shared/paths.js";

export type TacAuditEventType =
  | "OWNER_VIOLATION_BLOCKED"
  | "TOOL_OWNER_MISMATCH_BLOCKED"
  | "SKILL_DENIED_BY_POLICY"
  | "NON_RELEASED_HANDOFF_BLOCKED"
  | "PUBLISH_GATE_BLOCKED"
  | "FALLBACK_STEP_USED"
  | "FALLBACK_EXHAUSTED"
  | "CRITICAL_DATA_UNAVAILABLE_BLOCKED"
  | "DEGRADED_MODE_ENTERED"
  | "DEGRADED_MODE_EXITED";

export interface TacAuditEvent {
  event_id?: string;
  session_id: string;
  stage: string;
  ts?: string;
  event_type: TacAuditEventType;
  severity: "info" | "warning" | "error";
  actor_agent: string;
  decision: "allow" | "deny";
  reason_code: string;
  artifact_ref?: string;
}

export interface AuditWriteResult {
  accepted: boolean;
  file_path: string;
  serialized: string;
}

export interface AuditWriteOptions {
  auditDir?: string;
  now?: Date;
}

function formatDay(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export function writeAuditEvent(event: TacAuditEvent, options: AuditWriteOptions = {}): AuditWriteResult {
  const now = options.now ?? new Date();
  const ts = event.ts ?? now.toISOString();
  const eventId = event.event_id ?? randomUUID();
  const auditDir = resolveTacAuditDir(options.auditDir);
  fs.mkdirSync(auditDir, { recursive: true });
  const filePath = path.join(auditDir, `${formatDay(new Date(ts))}.jsonl`);
  const normalized = {
    event_id: eventId,
    session_id: event.session_id,
    stage: event.stage,
    ts,
    event_type: event.event_type,
    severity: event.severity,
    actor_agent: event.actor_agent,
    decision: event.decision,
    reason_code: event.reason_code,
    artifact_ref: event.artifact_ref ?? null,
  };
  const serialized = `${JSON.stringify(normalized)}\n`;
  fs.appendFileSync(filePath, serialized, "utf8");
  return {
    accepted: true,
    file_path: filePath,
    serialized: serialized.trimEnd(),
  };
}
