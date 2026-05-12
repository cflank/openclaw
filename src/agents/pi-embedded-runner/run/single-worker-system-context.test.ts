import { describe, expect, it } from "vitest";
import { buildEmbeddedSystemPrompt } from "../system-prompt.js";
import { createRuntimeContext, type SingleWorkerCommand } from "./params.js";
import { buildSingleWorkerMinimalSystemContext } from "./single-worker-system-context.js";

function buildCommand(): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    worker_id: "market_analyst",
    profile: "CN_A",
    stage: "frontline",
    run_id: "run-1",
    call_id: "call-1",
    runtime_vars: {
      ticker: "AAPL",
      company_name: "Apple",
      market: "US",
      currency: "USD",
      currency_symbol: "$",
      current_date: "2026-05-06",
      start_date: "2026-04-06",
      end_date: "2026-05-06",
    },
    allowed_tools: ["market_market_data_pack", "openviking_write_material"],
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
    system_context_policy: "single_worker_minimal",
  };
}

describe("single worker system context policy", () => {
  it("keeps default embedded system prompt content unchanged when no policy override is used", () => {
    const prompt = buildEmbeddedSystemPrompt({
      workspaceDir: "/tmp/openclaw",
      reasoningTagHint: false,
      runtimeInfo: {
        host: "local",
        os: "darwin",
        arch: "arm64",
        node: process.version,
        model: "gpt-5.4",
        provider: "openai",
      },
      tools: [{ name: "market_market_data_pack" } as { name: string }],
      modelAliasLines: [],
      userTimezone: "UTC",
      promptMode: "minimal",
      contextFiles: [
        { path: "/tmp/openclaw/TOOLS.md", content: "# TOOLS.md\nTool notes." },
        { path: "/tmp/openclaw/SOUL.md", content: "# SOUL.md\nPersona notes." },
        { path: "/tmp/openclaw/HEARTBEAT.md", content: "# HEARTBEAT.md\nHeartbeat task." },
      ],
    });

    expect(prompt).toContain("## OpenClaw CLI Quick Reference");
    expect(prompt).toContain("TOOLS.md does not control tool availability");
    expect(prompt).toContain("/tmp/openclaw/TOOLS.md");
    expect(prompt).toContain("/tmp/openclaw/SOUL.md");
    expect(prompt).toContain("/tmp/openclaw/HEARTBEAT.md");
  });

  it("builds minimal single-worker system context without CLI quick reference and bootstrap file guidance", () => {
    const context = createRuntimeContext({
      command: buildCommand(),
      openclawRunId: "openclaw-run-1",
    });
    const prompt = buildSingleWorkerMinimalSystemContext({
      context,
      tools: [{ name: "market_market_data_pack" } as { name: string }],
      workspaceDir: "/tmp/openclaw/agents/market_analyst",
    });

    expect(prompt).toBe("");
    expect(prompt).not.toContain("你正在执行当前分析师的一轮任务。");
    expect(prompt).not.toContain("请按用户消息中的角色、工具和报告格式要求完成本轮分析。");
    expect(prompt).not.toContain("worker=market_analyst | stage=frontline | profile=CN_A");
    expect(prompt).not.toContain("openclaw_run_id=openclaw-run-1 | run_id=run-1 | call_id=call-1");
    expect(prompt).not.toContain("## Report Submission");
    expect(prompt).not.toContain("visible report-writing tool");
    expect(prompt).not.toContain("write_tool_args=");
    expect(prompt).not.toContain("l1_uri=");
    expect(prompt).not.toContain("l2_prefix=");
    expect(prompt).not.toContain("## Visible Tools");
    expect(prompt).not.toContain("- market_market_data_pack");
    expect(prompt).not.toContain("## OpenClaw CLI Quick Reference");
    expect(prompt).not.toContain("TOOLS.md does not control tool availability");
    expect(prompt).not.toContain("HEARTBEAT");
    expect(prompt).not.toContain("SOUL.md");
  });
});
