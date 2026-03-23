import path from "node:path";
import { describe, expect, it } from "vitest";
import { runGate } from "../runtime/gates/gate-runner.js";
import { makeTempDir, readJsonLines } from "./test-helpers.js";

describe("publish gate blocked", () => {
  it("blocks publish when final_report is not released and writes audit", () => {
    const root = makeTempDir("tac-publish");
    const auditDir = path.join(root, "audit");
    const contractsDir = path.join(root, "contracts");

    const result = runGate(
      {
        gate: "publish_gate",
        session_id: "s3",
        stage: "publish",
        actor_agent: "research_manager",
        candidate_artifact: {
          artifact_id: "final_report",
          session_id: "s3",
          stage: "final_report_assembly",
          as_of_ts: new Date().toISOString(),
          release_status: "draft",
          payload: {},
        },
      },
      { contractsDir, auditDir },
    );

    expect(result.passed).toBe(false);
    expect(result.mandatory_audit_event).toBe("PUBLISH_GATE_BLOCKED");

    const dayFile = path.join(auditDir, `${new Date().toISOString().slice(0, 10)}.jsonl`);
    const rows = readJsonLines(dayFile);
    expect(rows.some((r) => r.event_type === "PUBLISH_GATE_BLOCKED")).toBe(true);
  });
});

