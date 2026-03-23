import fs from "node:fs";
import {
  hasReleasedArtifact,
  readLatestReleasedArtifact,
  type ContractStoreOptions,
} from "../contracts/contract-store.js";
import type { ContractArtifact } from "../contracts/contract-types.js";
import { validateRelease } from "../release/release-validator.js";
import { writeAuditEvent } from "../audit/audit-writer.js";
import { resolveTacGateThresholdPath } from "../shared/paths.js";

type GateName = "review_gate" | "risk_gate" | "publish_gate";

interface GateConfig {
  required_artifacts: string[];
  fail_closed: boolean;
  required_verdict_schema_valid?: boolean;
  final_report_must_be_released?: boolean;
  block_event?: string;
}

interface GateConfigFile {
  gates: Record<GateName, GateConfig>;
}

export interface GateRunInput {
  gate: GateName;
  session_id: string;
  stage: string;
  actor_agent: string;
  candidate_artifact?: ContractArtifact;
}

export interface GateRunOptions extends ContractStoreOptions {
  gateConfigPath?: string;
  auditDir?: string;
  schemaPath?: string;
}

export interface GateRunResult {
  passed: boolean;
  reason_code: string;
  mandatory_audit_event?: string;
  missing_artifacts: string[];
}

let cachedPath: string | null = null;
let cachedConfig: GateConfigFile | null = null;

function loadGateConfig(pathOverride?: string): GateConfigFile {
  const p = resolveTacGateThresholdPath(pathOverride);
  if (cachedConfig && cachedPath === p) {
    return cachedConfig;
  }
  const parsed = JSON.parse(fs.readFileSync(p, "utf8")) as GateConfigFile;
  cachedPath = p;
  cachedConfig = parsed;
  return parsed;
}

function block(
  input: GateRunInput,
  reasonCode: string,
  eventType: string,
  options: GateRunOptions,
  artifactRef?: string,
): GateRunResult {
  writeAuditEvent(
    {
      session_id: input.session_id,
      stage: input.stage,
      event_type: eventType as
        | "OWNER_VIOLATION_BLOCKED"
        | "TOOL_OWNER_MISMATCH_BLOCKED"
        | "SKILL_DENIED_BY_POLICY"
        | "NON_RELEASED_HANDOFF_BLOCKED"
        | "PUBLISH_GATE_BLOCKED"
        | "FALLBACK_STEP_USED"
        | "FALLBACK_EXHAUSTED"
        | "CRITICAL_DATA_UNAVAILABLE_BLOCKED"
        | "DEGRADED_MODE_ENTERED"
        | "DEGRADED_MODE_EXITED",
      severity: "error",
      actor_agent: input.actor_agent,
      decision: "deny",
      reason_code: reasonCode,
      artifact_ref: artifactRef,
    },
    { auditDir: options.auditDir },
  );
  return {
    passed: false,
    reason_code: reasonCode,
    mandatory_audit_event: eventType,
    missing_artifacts: [],
  };
}

export function runGate(input: GateRunInput, options: GateRunOptions = {}): GateRunResult {
  const cfg = loadGateConfig(options.gateConfigPath);
  const gateCfg = cfg.gates[input.gate];
  const missing: string[] = [];

  for (const artifactId of gateCfg.required_artifacts) {
    if (input.candidate_artifact?.artifact_id === artifactId) {
      continue;
    }
    if (!hasReleasedArtifact(input.session_id, artifactId, options)) {
      missing.push(artifactId);
    }
  }

  if (missing.length > 0) {
    const event = gateCfg.block_event ?? "CRITICAL_DATA_UNAVAILABLE_BLOCKED";
    writeAuditEvent(
      {
        session_id: input.session_id,
        stage: input.stage,
        event_type: event as
          | "OWNER_VIOLATION_BLOCKED"
          | "TOOL_OWNER_MISMATCH_BLOCKED"
          | "SKILL_DENIED_BY_POLICY"
          | "NON_RELEASED_HANDOFF_BLOCKED"
          | "PUBLISH_GATE_BLOCKED"
          | "FALLBACK_STEP_USED"
          | "FALLBACK_EXHAUSTED"
          | "CRITICAL_DATA_UNAVAILABLE_BLOCKED"
          | "DEGRADED_MODE_ENTERED"
          | "DEGRADED_MODE_EXITED",
        severity: "error",
        actor_agent: input.actor_agent,
        decision: "deny",
        reason_code: "required_artifacts_missing",
        artifact_ref: missing.join(","),
      },
      { auditDir: options.auditDir },
    );
    return {
      passed: false,
      reason_code: "required_artifacts_missing",
      mandatory_audit_event: event,
      missing_artifacts: missing,
    };
  }

  if (input.gate === "risk_gate") {
    const artifact =
      input.candidate_artifact ??
      (readLatestReleasedArtifact(input.session_id, "risk_verdict", options) as unknown as ContractArtifact | null);
    if (!artifact) {
      return block(input, "risk_verdict_missing", "CRITICAL_DATA_UNAVAILABLE_BLOCKED", options, "risk_verdict");
    }
    const validation = validateRelease(
      {
        artifact,
        action: "risk_gate",
      },
      { schemaPath: options.schemaPath },
    );
    if (!validation.allowed || (gateCfg.required_verdict_schema_valid && !validation.schema_valid)) {
      return block(
        input,
        validation.reason_code,
        validation.mandatory_audit_event ?? "CRITICAL_DATA_UNAVAILABLE_BLOCKED",
        options,
        artifact.artifact_id,
      );
    }
  }

  if (input.gate === "publish_gate") {
    const artifact =
      input.candidate_artifact ??
      (readLatestReleasedArtifact(input.session_id, "final_report", options) as unknown as ContractArtifact | null);
    if (!artifact) {
      return block(input, "final_report_missing", "PUBLISH_GATE_BLOCKED", options, "final_report");
    }
    if (gateCfg.final_report_must_be_released && artifact.release_status !== "released") {
      return block(input, "final_report_not_released", "PUBLISH_GATE_BLOCKED", options, artifact.artifact_id);
    }
  }

  return {
    passed: true,
    reason_code: "gate_passed",
    missing_artifacts: [],
  };
}

