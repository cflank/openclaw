import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { makeTempDir } from "./test-helpers.js";
import { loadRecoveryState } from "../runtime/recovery/recovery-loader.js";

describe("recovery-loader", () => {
  it("restores latest state from checkpoint/contracts/audit", () => {
    const root = makeTempDir("tac-recovery");
    const checkpoints = path.join(root, "checkpoints");
    const contracts = path.join(root, "contracts");
    const audit = path.join(root, "audit");
    fs.mkdirSync(checkpoints, { recursive: true });
    fs.mkdirSync(path.join(contracts, "s4", "trade_plan"), { recursive: true });
    fs.mkdirSync(audit, { recursive: true });

    fs.writeFileSync(
      path.join(checkpoints, "s4.json"),
      JSON.stringify({ session_id: "s4", current_stage: "risk_chain" }, null, 2),
      "utf8",
    );
    fs.writeFileSync(
      path.join(contracts, "s4", "trade_plan", "r1.json"),
      JSON.stringify({ session_id: "s4", artifact_id: "trade_plan", stage: "plan_chain" }, null, 2),
      "utf8",
    );
    fs.writeFileSync(
      path.join(audit, `${new Date().toISOString().slice(0, 10)}.jsonl`),
      `${JSON.stringify({
        event_id: "e1",
        session_id: "s4",
        stage: "risk_chain",
        ts: new Date().toISOString(),
        event_type: "FALLBACK_STEP_USED",
        severity: "info",
        actor_agent: "trader",
        decision: "allow",
        reason_code: "ok",
        artifact_ref: null,
      })}\n`,
      "utf8",
    );

    const result = loadRecoveryState({
      sessionId: "s4",
      checkpointDir: checkpoints,
      contractsDir: contracts,
      auditDir: audit,
    });

    expect(result.recovered).toBe(true);
    expect(result.next_stage).toBe("risk_chain");
    expect(result.needs_manual_reconcile).toBe(false);
  });
});

