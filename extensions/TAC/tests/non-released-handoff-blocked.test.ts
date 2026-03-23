import { describe, expect, it } from "vitest";
import { validateRelease } from "../runtime/release/release-validator.js";

describe("release-validator handoff", () => {
  it("blocks cross-agent handoff when artifact is not released", () => {
    const result = validateRelease({
      action: "handoff",
      artifact: {
        artifact_id: "trade_plan",
        session_id: "s2",
        stage: "plan_chain",
        as_of_ts: new Date().toISOString(),
        release_status: "draft",
        producer_agent: "trader",
        consumer_agent: "risk_manager",
      },
    });

    expect(result.allowed).toBe(false);
    expect(result.mandatory_audit_event).toBe("NON_RELEASED_HANDOFF_BLOCKED");
  });
});

