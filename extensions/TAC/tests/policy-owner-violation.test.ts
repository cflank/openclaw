import { describe, expect, it } from "vitest";
import { evaluatePolicy } from "../runtime/policy/policy-evaluator.js";

describe("policy-evaluator owner violation", () => {
  it("blocks unknown owner in tool call", () => {
    const result = evaluatePolicy({
      agentId: "market_analyst",
      stage: "frontline_chain",
      action: "tool_call",
      ownerId: "unknown-owner",
      toolName: "get_cn_quote",
    });
    expect(result.decision).toBe("deny");
    expect(result.reason_code).toBe("owner_violation");
    expect(result.mandatory_audit_event).toBe("OWNER_VIOLATION_BLOCKED");
  });
});

