import { Type } from "@sinclair/typebox";
import { describe, expect, it } from "vitest";
import type { AnyAgentTool } from "../../pi-tools.types.js";
import {
  assertSingleWorkerAllowedToolsRegistered,
  appendSingleWorkerMaterialBriefToPrompt,
  mergeRuntimeMcpConfig,
  prependSingleWorkerProfilePromptToPrompt,
  prepareSingleWorkerRuntimeTools,
  resolveSingleWorkerMcpConfigOverride,
  resolveRuntimeToolCallSuccessRecordFields,
  resolveRuntimeToolCallErrorMessage,
  resolveRuntimeToolCallStatus,
  resolveRuntimeWriteReceiptPathForRecord,
  resolveToolResultDetails,
} from "./attempt.js";
import {
  createRuntimeContext,
  SINGLE_WORKER_DEFAULT_PROMPT,
  type SingleWorkerCommand,
} from "./params.js";

function makeTool(name: string): AnyAgentTool {
  return {
    name,
    label: name,
    description: name,
    parameters: Type.Object({}, { additionalProperties: true }),
    execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
  };
}

function readToolSchemaProperties(tool: AnyAgentTool | undefined): Record<string, unknown> {
  const parameters = (tool?.parameters ?? {}) as Record<string, unknown>;
  const properties = parameters.properties;
  if (properties && typeof properties === "object" && !Array.isArray(properties)) {
    return properties as Record<string, unknown>;
  }
  return {};
}

function readSchemaObjectProperty(
  properties: Record<string, unknown>,
  key: string,
): Record<string, unknown> {
  const value = properties[key];
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function makeCommand(allowedTools: string[]): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    run_id: "run-1",
    call_id: "call-1",
    worker_id: "market_analyst",
    stage: "frontline",
    profile: "CN_A",
    runtime_vars: { ticker: "AAPL" },
    allowed_tools: allowedTools,
    evidence_dir: "/tmp/evidence",
    upstream_materials: [],
    openviking_read_capabilities: [],
    material_target: {
      run_id: "run-1",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      target_name: "report",
      l1_uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
      l2_prefix: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/evidence/",
    },
    read_policy: {
      default_layer: "L1",
      allow_l2_when: ["chart_required"],
      forbid_compact_as_writing_source: true,
    },
    stop_after_first_response: false,
  };
}

describe("single-worker runtime tool preparation", () => {
  it("adds openviking runtime tools and keeps allowlist narrowing authoritative", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["custom.local_tool", "openviking_write_material"]),
      openclawRunId: "oc-run-1",
    });
    const prepared = prepareSingleWorkerRuntimeTools({
      tools: [makeTool("custom.local_tool")],
      runtimeContext,
      toolsAllow: [
        "custom.local_tool",
        "openviking_write_material",
        "openviking_read_with_capability",
      ],
    });

    expect(prepared.map((tool) => tool.name)).toEqual([
      "custom.local_tool",
      "openviking_write_material",
    ]);
  });

  it("uses intersection of runtime allowed_tools and params.toolsAllow", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["openviking_write_material", "openviking_read_with_capability"]),
      openclawRunId: "oc-run-2",
    });
    const prepared = prepareSingleWorkerRuntimeTools({
      tools: [],
      runtimeContext,
      toolsAllow: ["openviking_read_with_capability"],
    });

    expect(prepared.map((tool) => tool.name)).toEqual(["openviking_read_with_capability"]);
  });

  it("hides pm_decision from non-PM write_material schema", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["openviking_write_material"]),
      openclawRunId: "oc-run-pm-schema-non-pm",
    });
    const prepared = prepareSingleWorkerRuntimeTools({
      tools: [],
      runtimeContext,
      toolsAllow: ["openviking_write_material"],
    });
    const writeTool = prepared.find((tool) => tool.name === "openviking_write_material");
    expect(writeTool).toBeDefined();
    const properties = readToolSchemaProperties(writeTool);
    expect(Object.prototype.hasOwnProperty.call(properties, "pm_decision")).toBe(false);
    expect(Object.keys(properties)).toEqual(["content"]);
  });

  it("keeps pm_decision in PM write_material schema", () => {
    const command = makeCommand(["openviking_write_material"]);
    command.agent = "portfolio_manager";
    command.worker_id = "portfolio_manager";
    command.stage = "portfolio_decision";
    command.material_target.worker_id = "portfolio_manager";
    command.material_target.stage = "portfolio_decision";
    command.material_target.l1_uri =
      "viking://resources/workflow/run-1/portfolio_decision/portfolio_manager/call-1/report.md";
    command.material_target.l2_prefix =
      "viking://resources/workflow/run-1/portfolio_decision/portfolio_manager/call-1/evidence/";

    const runtimeContext = createRuntimeContext({
      command,
      openclawRunId: "oc-run-pm-schema-pm",
    });
    const prepared = prepareSingleWorkerRuntimeTools({
      tools: [],
      runtimeContext,
      toolsAllow: ["openviking_write_material"],
    });
    const writeTool = prepared.find((tool) => tool.name === "openviking_write_material");
    expect(writeTool).toBeDefined();
    const properties = readToolSchemaProperties(writeTool);
    expect(Object.prototype.hasOwnProperty.call(properties, "pm_decision")).toBe(true);
    expect(Object.keys(properties).sort()).toEqual(["content", "pm_decision"]);
    const pmDecisionSchema = readSchemaObjectProperty(properties, "pm_decision");
    const pmDecisionProperties = readSchemaObjectProperty(pmDecisionSchema, "properties");
    const ratingSchema = readSchemaObjectProperty(pmDecisionProperties, "rating");
    const ratingAnyOf = Array.isArray(ratingSchema.anyOf) ? ratingSchema.anyOf : [];
    const ratingEnum = ratingAnyOf
      .map((item) =>
        item && typeof item === "object" && !Array.isArray(item)
          ? (item as Record<string, unknown>).const
          : undefined,
      )
      .filter((item): item is string => typeof item === "string");
    expect(ratingEnum).toEqual(["buy", "hold", "sell", "neutral", "not_rated"]);
  });

  it("narrows read_with_capability model-visible schema to material_id + layer", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["openviking_read_with_capability"]),
      openclawRunId: "oc-run-read-schema",
    });
    const prepared = prepareSingleWorkerRuntimeTools({
      tools: [],
      runtimeContext,
      toolsAllow: ["openviking_read_with_capability"],
    });
    const readTool = prepared.find((tool) => tool.name === "openviking_read_with_capability");
    expect(readTool).toBeDefined();
    const properties = readToolSchemaProperties(readTool);
    expect(Object.keys(properties).sort()).toEqual(["layer", "material_id"]);
  });

  it("keeps upstream material refs out of prompt when runtimeContext exists", () => {
    const command = makeCommand(["openviking_read_with_capability"]);
    command.upstream_materials = [
      {
        material_id: "mat-1",
        capability_id: "cap-1",
        worker_id: "market_analyst",
        stage: "frontline",
        l1_uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-0/report.md",
        l1_sha256: "abc123",
        l2_allowed_prefix:
          "viking://resources/workflow/run-1/frontline/market_analyst/call-0/evidence/",
        call_id: "call-0",
      },
    ];
    const runtimeContext = createRuntimeContext({
      command,
      openclawRunId: "oc-run-3",
    });
    const prompt = appendSingleWorkerMaterialBriefToPrompt("原始提示词", runtimeContext);
    expect(prompt).toBe("原始提示词");
    expect(prompt).not.toContain("[ApprovedMaterials]");
    expect(prompt).not.toContain("[ReportSubmission]");
    expect(prompt).not.toContain("visible report-writing tool");
    expect(prompt).not.toContain(
      "uri=viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    );
    expect(prompt).not.toContain("target_name=report");
    expect(prompt).not.toContain("material_id=mat-1");
    expect(prompt).not.toContain("recommended_read=material_id+layer(L1)");
    expect(prompt).not.toContain("layer=L1");
    expect(prompt).not.toContain("l1_sha256=abc123");
    expect(prompt).not.toContain("上游正文");
  });

  it("prepends rendered profile prompt before runtime instruction text", () => {
    const prompt = prependSingleWorkerProfilePromptToPrompt(
      SINGLE_WORKER_DEFAULT_PROMPT,
      "Ticker=AAPL Company=Apple Currency=USD Date=2026-05-03",
    );
    expect(prompt.startsWith("Ticker=AAPL Company=Apple Currency=USD Date=2026-05-03")).toBe(true);
    expect(prompt).not.toContain(SINGLE_WORKER_DEFAULT_PROMPT);
  });

  it("keeps prompt unchanged when runtimeContext is absent", () => {
    const prompt = appendSingleWorkerMaterialBriefToPrompt("原始提示词");
    expect(prompt).toBe("原始提示词");
  });

  it("fails fast when allowed tool is missing from runtime registrations", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["missing.local.tool", "openviking_write_material"]),
      openclawRunId: "oc-run-missing-tool",
    });
    expect(() =>
      prepareSingleWorkerRuntimeTools({
        tools: [],
        runtimeContext,
        toolsAllow: ["missing.local.tool", "openviking_write_material"],
      }),
    ).toThrowError(/missing runtime registration/i);
  });

  it("fails closed after effective tools are finalized", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["custom.local_tool", "openviking_write_material"]),
      openclawRunId: "oc-run-final-check",
    });
    expect(() =>
      assertSingleWorkerAllowedToolsRegistered({
        runtimeContext,
        toolsAllow: ["custom.local_tool", "openviking_write_material"],
        registeredTools: [makeTool("openviking_write_material")],
        allowDeferredBundleMcpTools: false,
      }),
    ).toThrowError(/missing runtime registration/i);
  });
});

describe("single-worker openviking MCP config", () => {
  it("does not build MCP override for non-bundle tools", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["custom.local_tool", "openviking_write_material"]),
      openclawRunId: "oc-run-mcp-override",
    });
    const override = resolveSingleWorkerMcpConfigOverride(runtimeContext);
    expect(override).toBeUndefined();
  });

  it("merges runtime MCP override without dropping existing mcp.servers", () => {
    const runtimeContext = createRuntimeContext({
      command: makeCommand(["openvikingArtifact__ov_mkt__stock_price"]),
      openclawRunId: "oc-run-mcp-merge",
    });
    const merged = mergeRuntimeMcpConfig({
      cfg: {
        mcp: {
          servers: {
            existing: { transport: "streamable-http", url: "http://127.0.0.1:9999/mcp" },
          },
        },
      },
      runtimeContext,
    });
    expect(merged?.mcp?.servers?.existing).toBeDefined();
    expect(merged?.mcp?.servers?.openvikingArtifact).toBeDefined();
  });
});

describe("runtime tool result details", () => {
  it("reads OpenViking receipt/sha fields from AgentToolResult.details", () => {
    const details = resolveToolResultDetails({
      content: [{ type: "text", text: "ok" }],
      details: {
        receipt_path: "/tmp/openviking-write-receipt.json",
        uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
        sha256: "abc123",
      },
    });
    expect(details).toMatchObject({
      receipt_path: "/tmp/openviking-write-receipt.json",
      sha256: "abc123",
    });
  });

  it("falls back to top-level object for non-AgentToolResult payloads", () => {
    const details = resolveToolResultDetails({
      uri: "viking://resources/workflow/a",
      sha256: "xyz",
    });
    expect(details).toMatchObject({ sha256: "xyz" });
  });

  it("parses JSON text content when details field is absent", () => {
    const details = resolveToolResultDetails({
      content: [
        {
          type: "text",
          text: JSON.stringify({
            receipt_path: "/tmp/openviking-write-receipt.json",
            uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
            sha256: "abc123",
          }),
        },
      ],
    });
    expect(details).toMatchObject({
      receipt_path: "/tmp/openviking-write-receipt.json",
      sha256: "abc123",
    });
  });

  it("does not synthesize fake material_id/capability_id for write receipts", () => {
    const resultDetails = resolveToolResultDetails({
      content: [{ type: "text", text: "ok" }],
      details: {
        receipt_path: "/tmp/openviking-write-receipt.json",
        uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
        sha256: "abc123",
      },
    });
    const fields = resolveRuntimeToolCallSuccessRecordFields({
      argsRecord: {
        uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
      },
      resultDetails,
    });
    expect(fields).toMatchObject({
      uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    });
    expect(fields.material_id).toBeUndefined();
    expect(fields.capability_id).toBeUndefined();
  });

  it("requires receipt_path for openviking_write_material success records", () => {
    expect(() =>
      resolveRuntimeWriteReceiptPathForRecord({
        toolName: "openviking_write_material",
        resultDetails: {},
      }),
    ).toThrowError(/missing receipt_path/i);
    expect(
      resolveRuntimeWriteReceiptPathForRecord({
        toolName: "openviking_write_material",
        resultDetails: { receipt_path: "/tmp/openviking-write-receipt.json" },
      }),
    ).toBe("/tmp/openviking-write-receipt.json");
    expect(
      resolveRuntimeWriteReceiptPathForRecord({
        toolName: "openviking_write_material",
        resultDetails: { receiptPath: "/tmp/openviking-write-receipt-camel.json" },
      }),
    ).toBe("/tmp/openviking-write-receipt-camel.json");
  });

  it("ignores receipt_path requirement for non-write tools", () => {
    expect(
      resolveRuntimeWriteReceiptPathForRecord({
        toolName: "custom.local_tool",
        resultDetails: {},
      }),
    ).toBeUndefined();
  });

  it("marks tool-call record as error when result details indicates failure", () => {
    expect(resolveRuntimeToolCallStatus({ resultDetails: { ok: false } })).toBe("error");
    expect(resolveRuntimeToolCallStatus({ resultDetails: { success: false } })).toBe("error");
    expect(resolveRuntimeToolCallStatus({ resultDetails: { status: "error" } })).toBe("error");
    expect(resolveRuntimeToolCallStatus({ resultDetails: { status: "failed" } })).toBe("error");
    expect(resolveRuntimeToolCallStatus({ resultDetails: { ok: true } })).toBe("success");
    expect(resolveRuntimeToolCallStatus({ resultDetails: { status: "success" } })).toBe("success");
    expect(resolveRuntimeToolCallStatus({ resultDetails: {} })).toBe("success");
  });

  it("extracts error message from runtime tool result details", () => {
    expect(
      resolveRuntimeToolCallErrorMessage({
        resultDetails: {
          status: "error",
          error: "openviking_read_with_capability capability not found in command",
        },
      }),
    ).toBe("openviking_read_with_capability capability not found in command");
    expect(
      resolveRuntimeToolCallErrorMessage({
        resultDetails: {
          ok: false,
          error: { message: "tool failed by nested message" },
        },
      }),
    ).toBe("tool failed by nested message");
    expect(
      resolveRuntimeToolCallErrorMessage({
        resultDetails: { ok: false },
        fallback: "fallback message",
      }),
    ).toBe("fallback message");
  });
});
