import { describe, expect, it } from "vitest";
import { evaluatePolicy } from "../runtime/policy/policy-evaluator.js";

describe("policy-evaluator tool owner mismatch", () => {
  it("blocks get_event_window when owner is not market-news-mcp", () => {
    const result = evaluatePolicy({
      agentId: "market_analyst",
      stage: "frontline_chain",
      action: "tool_call",
      ownerId: "market-cn-mcp",
      toolName: "get_event_window",
    });
    expect(result.decision).toBe("deny");
    expect(result.reason_code).toBe("tool_owner_mismatch");
    expect(result.mandatory_audit_event).toBe("TOOL_OWNER_MISMATCH_BLOCKED");
  });
});

