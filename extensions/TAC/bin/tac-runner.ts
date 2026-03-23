import { randomUUID } from "node:crypto";
import { evaluatePolicy } from "../runtime/policy/policy-evaluator.js";
import { runGate } from "../runtime/gates/gate-runner.js";
import type { ContractArtifact } from "../runtime/contracts/contract-types.js";
import { writeReleasedArtifact } from "../runtime/contracts/contract-store.js";
import { writeAuditEvent } from "../runtime/audit/audit-writer.js";

interface SmokeCaseResult {
  case_id: string;
  passed: boolean;
  details: Record<string, unknown>;
}

function validRiskVerdict(sessionId: string): ContractArtifact {
  return {
    artifact_id: "risk_verdict",
    session_id: sessionId,
    stage: "risk_chain",
    as_of_ts: new Date().toISOString(),
    release_status: "released",
    producer_agent: "risk_manager",
    payload: {
      session_id: sessionId,
      stage: "risk_chain",
      as_of_ts: new Date().toISOString(),
      verdict: "APPROVE",
      risk_level: "MEDIUM",
      degraded_mode: "NONE",
      confidence: 0.8,
      blocking_reasons: [],
      required_actions: [],
      evidence_refs: [],
    },
  };
}

function invalidRiskVerdict(sessionId: string): ContractArtifact {
  return {
    artifact_id: "risk_verdict",
    session_id: sessionId,
    stage: "risk_chain",
    as_of_ts: new Date().toISOString(),
    release_status: "released",
    producer_agent: "risk_manager",
    payload: {
      session_id: sessionId,
      stage: "risk_chain",
      as_of_ts: new Date().toISOString(),
      verdict: "INVALID_ENUM",
      risk_level: "MEDIUM",
      degraded_mode: "NONE",
      confidence: 0.8,
      blocking_reasons: [],
    },
  };
}

async function runSmoke(): Promise<{ ok: boolean; results: SmokeCaseResult[] }> {
  const sessionId = `tac-smoke-${Date.now()}`;
  const results: SmokeCaseResult[] = [];

  const rv = validRiskVerdict(sessionId);
  writeReleasedArtifact(rv);
  const riskPass = runGate({
    gate: "risk_gate",
    session_id: sessionId,
    stage: "risk_chain",
    actor_agent: "risk_manager",
    candidate_artifact: { ...rv, ...rv.payload },
  });
  results.push({
    case_id: "smoke-1-valid-risk-verdict",
    passed: riskPass.passed,
    details: riskPass,
  });

  const rvBad = invalidRiskVerdict(sessionId);
  const riskFail = runGate({
    gate: "risk_gate",
    session_id: sessionId,
    stage: "risk_chain",
    actor_agent: "risk_manager",
    candidate_artifact: { ...rvBad, ...rvBad.payload },
  });
  results.push({
    case_id: "smoke-2-invalid-risk-verdict",
    passed: !riskFail.passed,
    details: riskFail,
  });

  const handoff = evaluatePolicy({
    agentId: "research_manager",
    stage: "final_report_assembly",
    action: "handoff",
    artifactId: "trade_plan",
    artifactReleased: false,
  });
  if (handoff.decision === "deny" && handoff.mandatory_audit_event) {
    writeAuditEvent({
      session_id: sessionId,
      stage: "final_report_assembly",
      event_type: handoff.mandatory_audit_event,
      severity: "error",
      actor_agent: "research_manager",
      decision: "deny",
      reason_code: handoff.reason_code,
      artifact_ref: "trade_plan",
    });
  }
  results.push({
    case_id: "smoke-3-non-released-handoff",
    passed: handoff.decision === "deny" && handoff.mandatory_audit_event === "NON_RELEASED_HANDOFF_BLOCKED",
    details: handoff,
  });

  const publish = evaluatePolicy({
    agentId: "research_manager",
    stage: "publish",
    action: "publish",
    finalReportReleased: false,
  });
  if (publish.decision === "deny" && publish.mandatory_audit_event) {
    writeAuditEvent({
      session_id: sessionId,
      stage: "publish",
      event_type: publish.mandatory_audit_event,
      severity: "error",
      actor_agent: "research_manager",
      decision: "deny",
      reason_code: publish.reason_code,
      artifact_ref: "final_report",
    });
  }
  results.push({
    case_id: "smoke-4-publish-gate-blocked",
    passed: publish.decision === "deny" && publish.mandatory_audit_event === "PUBLISH_GATE_BLOCKED",
    details: publish,
  });

  return {
    ok: results.every((r) => r.passed),
    results,
  };
}

async function main(): Promise<void> {
  const args = new Set(process.argv.slice(2));
  if (!args.has("--smoke")) {
    // Default behavior for Batch-2.
    args.add("--smoke");
  }
  if (args.has("--smoke")) {
    const out = await runSmoke();
    process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
    process.exitCode = out.ok ? 0 : 1;
  }
}

main().catch((err) => {
  process.stderr.write(`tac-runner fatal: ${String(err)}\n`);
  process.exitCode = 1;
});
