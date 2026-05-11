import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  cleanupTempPaths,
  createContextEngineAttemptRunner,
  getHoisted,
  resetEmbeddedAttemptHarness,
} from "./attempt.spawn-workspace.test-support.js";
import type { SingleWorkerCommand } from "./params.js";

function makeSingleWorkerCommand(params: {
  evidenceDir: string;
  allowedTools: string[];
}): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    run_id: "run-1",
    call_id: "call-1",
    worker_id: "market_analyst",
    stage: "frontline",
    profile: "CN_A",
    runtime_vars: { ticker: "600519.SH" },
    allowed_tools: params.allowedTools,
    evidence_dir: params.evidenceDir,
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

describe("runEmbeddedAttempt toolsAllow startup cost", () => {
  const tempPaths: string[] = [];

  beforeEach(() => {
    resetEmbeddedAttemptHarness();
  });

  afterEach(async () => {
    await cleanupTempPaths(tempPaths);
  });

  it("keeps plugin-only allowlists on the shared tool policy path", async () => {
    const hoisted = getHoisted();
    hoisted.createOpenClawCodingToolsMock.mockReturnValue([
      {
        name: "memory_search",
        description: "search memory",
        parameters: { type: "object", properties: {} },
        execute: async () => "ok",
      },
      {
        name: "plugin_extra",
        description: "extra plugin tool",
        parameters: { type: "object", properties: {} },
        execute: async () => "ok",
      },
    ]);

    await createContextEngineAttemptRunner({
      contextEngine: {
        assemble: async ({ messages }) => ({ messages, estimatedTokens: 1 }),
      },
      attemptOverrides: {
        toolsAllow: ["memory_search"],
      },
      sessionKey: "agent:main:main",
      tempPaths,
    });

    expect(hoisted.createOpenClawCodingToolsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        includeCoreTools: false,
        runtimeToolAllowlist: ["memory_search"],
      }),
    );
    const createSessionOptions = hoisted.createAgentSessionMock.mock.calls[0]?.[0] as
      | { customTools?: { name: string }[] }
      | undefined;
    expect(createSessionOptions?.customTools?.map((tool) => tool.name)).toEqual(["memory_search"]);
  });

  it("uses single-worker allowed tools when building current-turn plugin tools", async () => {
    const hoisted = getHoisted();
    const evidenceDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-single-worker-tools-"));
    const workspaceDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "openclaw-single-worker-workspace-"),
    );
    tempPaths.push(evidenceDir);
    tempPaths.push(workspaceDir);
    await fs.mkdir(path.join(workspaceDir, "prompts"), { recursive: true });
    await fs.mkdir(path.join(workspaceDir, "skills"), { recursive: true });
    await fs.writeFile(path.join(workspaceDir, "IDENTITY.md"), "market analyst\n", "utf8");
    await fs.writeFile(path.join(workspaceDir, "skills", "manifest.yaml"), "skills: []\n", "utf8");
    await fs.writeFile(
      path.join(workspaceDir, "STAGES.yaml"),
      [
        "stage: frontline",
        "profiles:",
        "  CN_A:",
        "    approved: true",
        "    prompt: prompts/CN_A.md",
        "",
      ].join("\n"),
      "utf8",
    );
    await fs.writeFile(
      path.join(workspaceDir, "prompts", "CN_A.md"),
      "Analyze {ticker}.\n",
      "utf8",
    );
    hoisted.createOpenClawCodingToolsMock.mockReturnValue([
      {
        name: "market_market_data_pack",
        description: "market pack",
        parameters: { type: "object", properties: {} },
        execute: async () => "ok",
      },
      {
        name: "news_news_data_pack",
        description: "news pack",
        parameters: { type: "object", properties: {} },
        execute: async () => "ok",
      },
    ]);

    const command = makeSingleWorkerCommand({
      evidenceDir,
      allowedTools: ["market_market_data_pack", "openviking_write_material"],
    });

    await createContextEngineAttemptRunner({
      contextEngine: {
        assemble: async ({ messages }) => ({ messages, estimatedTokens: 1 }),
      },
      attemptOverrides: {
        config: {
          agents: {
            defaults: { workspace: workspaceDir },
            list: [{ id: "market_analyst", workspace: workspaceDir, default: true }],
          },
        },
        singleWorkerCommand: command,
        workspaceDir,
      },
      sessionKey: "agent:market_analyst:main",
      tempPaths,
    });

    expect(hoisted.createOpenClawCodingToolsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        includeCoreTools: false,
        runtimeToolAllowlist: ["market_market_data_pack", "openviking_write_material"],
      }),
    );
    const createSessionOptions = hoisted.createAgentSessionMock.mock.calls[0]?.[0] as
      | { customTools?: { name: string }[] }
      | undefined;
    expect(createSessionOptions?.customTools?.map((tool) => tool.name)).toEqual([
      "market_market_data_pack",
      "openviking_write_material",
    ]);
  });
});
