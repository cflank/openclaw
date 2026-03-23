import { describe, expect, it } from "vitest";
import { executeFallback } from "../runtime/fallback/fallback-executor.js";

describe("fallback-executor order", () => {
  it("uses fixed order and returns first available source", () => {
    const result = executeFallback({
      market: "CN",
      sourceAvailability: {
        tushare: false,
        akshare: true,
      },
    });
    expect(result.step_used).toEqual(["tushare", "akshare"]);
    expect(result.exhausted).toBe(false);
    expect(result.result_source).toBe("akshare");
  });
});

