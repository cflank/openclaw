import { describe, expect, it } from "vitest";
import { validateRelease } from "../runtime/release/release-validator.js";

describe("release-validator risk_verdict schema", () => {
  it("blocks invalid risk_verdict payload", () => {
    const result = validateRelease({
      action: "risk_gate",
      artifact: {
        artifact_id: "risk_verdict",
        session_id: "s1",
        stage: "risk_chain",
        as_of_ts: new Date().toISOString(),
        release_status: "released",
        verdict: "INVALID_ENUM",
        risk_level: "HIGH",
        degraded_mode: "NONE",
        confidence: 0.7,
        blocking_reasons: [],
      },
    });

    expect(result.allowed).toBe(false);
    expect(result.reason_code).toBe("risk_verdict_schema_invalid");
    expect(result.schema_valid).toBe(false);
  });
});

