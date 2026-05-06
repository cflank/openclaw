import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  appendOpenVikingMaterialBrief,
  createRuntimeContext,
  ensureEvidenceDir,
  renderSingleWorkerPromptTemplate,
  resolveSingleWorkerProfilePrompt,
  renderOpenVikingMaterialBrief,
  renderRuntimeTargetBrief,
  renderOpenVikingWriteTargetBrief,
  withRuntimeMarkers,
  writeWorkspaceEvidence,
  type SingleWorkerCommand,
} from "./params.js";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

async function writeFile(filePath: string, content: string): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, content, "utf8");
}

function buildCommand(evidenceDir: string): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    run_id: "run-1",
    call_id: "call-1",
    worker_id: "market_analyst",
    stage: "frontline",
    profile: "CN_A",
    runtime_vars: {
      ticker: "AAPL",
    },
    allowed_tools: ["market_data"],
    evidence_dir: evidenceDir,
    upstream_materials: [],
    openviking_read_capabilities: [],
    material_target: {
      run_id: "run-1",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      target_name: "market_analysis_report",
      l1_uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
      l2_prefix: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/evidence/",
    },
    read_policy: {
      default_layer: "L1",
      allow_l2_when: ["chart_required"],
      forbid_compact_as_writing_source: true,
    },
    stop_after_first_response: true,
  };
}

describe("single-worker runtime context", () => {
  afterEach(async () => {
    await Promise.all(
      tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
  });

  it("adds run/call/worker/stage/profile/openclaw_run_id markers", () => {
    const command = buildCommand("/tmp/evidence");
    const context = createRuntimeContext({
      command,
      openclawRunId: "openclaw-run-1",
    });
    const marked = withRuntimeMarkers({ source: "test" }, context);
    expect(marked.runtime_markers).toEqual({
      run_id: "run-1",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      profile: "CN_A",
      openclaw_run_id: "openclaw-run-1",
    });
  });

  it("ensures evidence dir and writes workspace evidence without text body", async () => {
    const root = await makeTempDir("openclaw-single-worker-");
    const evidenceDir = path.join(root, "evidence");
    const workspaceRoot = path.join(root, "agents", "market_analyst");
    await writeFile(path.join(workspaceRoot, "IDENTITY.md"), "identity content");
    await writeFile(path.join(workspaceRoot, "skills", "manifest.yaml"), "skills: []\n");
    await writeFile(path.join(workspaceRoot, "STAGES.yaml"), "stage: frontline\n");
    await writeFile(path.join(workspaceRoot, "prompts", "CN_A.md"), "prompt body");
    const command = buildCommand(evidenceDir);
    await ensureEvidenceDir(command);
    const context = createRuntimeContext({
      command,
      openclawRunId: "openclaw-run-2",
    });
    const evidencePath = await writeWorkspaceEvidence({
      context,
      workspaceRoot,
      selectedStage: "frontline",
    });
    const written = JSON.parse(await fs.readFile(evidencePath, "utf8")) as Record<string, unknown>;
    expect(written.source).toBe("openclaw_workspace_loader");
    expect(written.runtime_markers).toMatchObject({
      run_id: "run-1",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      profile: "CN_A",
      openclaw_run_id: "openclaw-run-2",
    });
    const textAssets = (written.text_assets as Array<Record<string, unknown>> | undefined) ?? [];
    expect(textAssets.length).toBeGreaterThan(0);
    expect(JSON.stringify(written)).not.toContain("prompt body");
  });

  it("renders upstream material brief from command.upstream_materials without body text", () => {
    const command = buildCommand("/tmp/evidence");
    command.upstream_materials = [
      {
        material_id: "mat-1",
        capability_id: "cap-1",
        worker_id: "market_analyst",
        stage: "frontline",
        l1_uri: "viking://resources/workflow/run-0/frontline/market_analyst/call-0/report.md",
        l1_sha256: "abc123",
        l2_allowed_prefix:
          "viking://resources/workflow/run-0/frontline/market_analyst/call-0/evidence/",
        call_id: "call-0",
      },
    ];
    const brief = renderOpenVikingMaterialBrief(command);
    expect(brief).toContain("[OpenVikingReadableMaterials]");
    expect(brief).toContain("worker=market_analyst");
    expect(brief).toContain("stage=frontline");
    expect(brief).toContain("material_id=mat-1");
    expect(brief).toContain("recommended_read=material_id+layer(L1)");
    expect(brief).toContain("layer=L1");
    expect(brief).toContain("l1_sha256=abc123");
    expect(brief).toContain("l2_available=yes");
    expect(brief).toContain("call_id=call-0");
    expect(brief).not.toContain("上游正文");
  });

  it("renders write target brief with canonical L1 target uri", () => {
    const command = buildCommand("/tmp/evidence");
    const brief = renderOpenVikingWriteTargetBrief(command);
    expect(brief).toContain("[OpenVikingWriteTarget]");
    expect(brief).toContain("run_id=run-1");
    expect(brief).toContain("target_name=market_analysis_report");
    expect(brief).toContain(
      "uri=viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    );
    expect(brief).toContain("worker_id=market_analyst");
    expect(brief).toContain("stage=frontline");
    expect(brief).toContain("call_id=call-1");
  });

  it("renders runtime target brief from command.runtime_vars", () => {
    const command = buildCommand("/tmp/evidence");
    command.runtime_vars = {
      ticker: "AAPL",
      company_name: "Apple",
      market: "US",
      currency: "USD",
      currency_symbol: "$",
      current_date: "2026-05-03",
      start_date: "2026-05-03",
      end_date: "2026-05-03",
    };
    const brief = renderRuntimeTargetBrief(command);
    expect(brief).toContain("[RuntimeTarget]");
    expect(brief).toContain("ticker=AAPL");
    expect(brief).toContain("company_name=Apple");
    expect(brief).toContain("market=US");
    expect(brief).toContain("currency=USD");
    expect(brief).toContain("currency_symbol=$");
    expect(brief).toContain("current_date=2026-05-03");
    expect(brief).toContain("start_date=2026-05-03");
    expect(brief).toContain("end_date=2026-05-03");
  });

  it("appends material brief into prompt exactly once", () => {
    const command = buildCommand("/tmp/evidence");
    command.upstream_materials = [
      {
        material_id: "mat-2",
        capability_id: "cap-2",
        worker_id: "news_analyst",
        stage: "frontline",
        l1_uri: "viking://resources/workflow/run-0/frontline/news_analyst/call-2/report.md",
        l1_sha256: "def456",
        l2_allowed_prefix:
          "viking://resources/workflow/run-0/frontline/news_analyst/call-2/evidence/",
        call_id: "call-2",
      },
    ];
    const appended = appendOpenVikingMaterialBrief("原始提示词", command);
    expect(appended).toContain("[RuntimeTarget]");
    expect(appended).toContain("[OpenVikingReadableMaterials]");
    expect(appended).toContain("[OpenVikingWriteTarget]");
    expect(appended).toContain("ticker=AAPL");
    expect(appended).toContain("run_id=run-1");
    expect(appended).toContain(
      "uri=viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    );
    const appendedTwice = appendOpenVikingMaterialBrief(appended, command);
    expect(appendedTwice).toBe(appended);
  });

  it("replaces stale runtime sections with authoritative runtime sections", () => {
    const command = buildCommand("/tmp/evidence");
    command.upstream_materials = [
      {
        material_id: "mat-new",
        capability_id: "cap-new",
        worker_id: "market_analyst",
        stage: "frontline",
        l1_uri: "viking://resources/workflow/run-0/frontline/market_analyst/call-0/report.md",
        l1_sha256: "sha-new",
        l2_allowed_prefix:
          "viking://resources/workflow/run-0/frontline/market_analyst/call-0/evidence/",
        call_id: "call-0",
      },
    ];
    const prompt = [
      "原始提示词",
      "",
      "[RuntimeTarget]",
      "- ticker=OLD",
      "- market=OLD",
      "",
      "[OpenVikingReadableMaterials]",
      "- material_id=legacy",
      "",
      "[OpenVikingWriteTarget]",
      "- uri=viking://legacy/uri.md",
      "",
      "结尾说明",
    ].join("\n");
    const appended = appendOpenVikingMaterialBrief(prompt, command);
    expect(appended).toContain("原始提示词");
    expect(appended).toContain("结尾说明");
    expect(appended).not.toContain("ticker=OLD");
    expect(appended).not.toContain("material_id=legacy");
    expect(appended).not.toContain("viking://legacy/uri.md");
    expect(appended).toContain("ticker=AAPL");
    expect(appended).toContain("material_id=mat-new");
    expect(appended).toContain(
      "uri=viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    );
  });

  it("still appends real write target section when prompt only references header token in prose", () => {
    const command = buildCommand("/tmp/evidence");
    const promptWithReferenceOnly =
      "请在最终调用中使用 [OpenVikingWriteTarget]，不要自行猜测 URI。";
    const appended = appendOpenVikingMaterialBrief(promptWithReferenceOnly, command);
    expect(appended).toContain("[RuntimeTarget]");
    expect(appended).toContain("[OpenVikingReadableMaterials]");
    expect(appended).toContain("[OpenVikingWriteTarget]");
    expect(appended).toContain("run_id=run-1");
    expect(appended).toContain(
      "uri=viking://resources/workflow/run-1/frontline/market_analyst/call-1/report.md",
    );
    expect(appended).toContain("target_name=market_analysis_report");
  });

  it("still appends real runtime target section when prompt mentions runtime target in prose", () => {
    const command = buildCommand("/tmp/evidence");
    command.runtime_vars = {
      ticker: "AAPL",
      company_name: "Apple",
      market: "US",
      currency: "USD",
      currency_symbol: "$",
      current_date: "2026-05-03",
      start_date: "2026-05-03",
      end_date: "2026-05-03",
    };
    const promptWithRuntimeHint =
      "请使用 runtime target 里的 ticker/company，不要写 [RuntimeTarget] 占位结构。";
    const appended = appendOpenVikingMaterialBrief(promptWithRuntimeHint, command);
    expect(appended).toContain("[RuntimeTarget]");
    expect(appended).toContain("ticker=AAPL");
    expect(appended).toContain("company_name=Apple");
    expect(appended).not.toContain("{ticker}");
    expect(appended).not.toContain("{company_name}");
  });

  it("renders profile prompt from STAGES profile path with runtime vars", async () => {
    const root = await makeTempDir("openclaw-profile-prompt-");
    const workspaceRoot = path.join(root, "agents", "market_analyst");
    await writeFile(
      path.join(workspaceRoot, "STAGES.yaml"),
      [
        "worker: market_analyst",
        "stage: frontline",
        "profiles:",
        "  US:",
        "    approved: true",
        "    prompt: prompts/US.md",
      ].join("\n"),
    );
    await writeFile(
      path.join(workspaceRoot, "prompts", "US.md"),
      "Ticker={ticker} Company={company_name} Currency={currency} Date={current_date}",
    );
    const command = buildCommand("/tmp/evidence");
    command.profile = "US";
    command.stage = "frontline";
    command.runtime_vars = {
      ticker: "AAPL",
      company_name: "Apple",
      currency: "USD",
      current_date: "2026-05-03",
    };
    const resolved = await resolveSingleWorkerProfilePrompt({ workspaceRoot, command });
    expect(resolved.prompt).toContain("Ticker=AAPL");
    expect(resolved.prompt).toContain("Company=Apple");
    expect(resolved.prompt).toContain("Currency=USD");
    expect(resolved.prompt).toContain("Date=2026-05-03");
    expect(resolved.prompt).not.toContain("{ticker}");
    expect(resolved.promptTemplatePath.endsWith("/prompts/US.md")).toBe(true);
  });

  it("fails when runtime vars are missing for prompt placeholders", () => {
    expect(() =>
      renderSingleWorkerPromptTemplate("Ticker={ticker} Company={company_name}", {
        ticker: "AAPL",
      }),
    ).toThrowError(/runtime vars missing/i);
  });

  it("fails when profile is not approved in stage policy", async () => {
    const root = await makeTempDir("openclaw-profile-blocked-");
    const workspaceRoot = path.join(root, "agents", "market_analyst");
    await writeFile(
      path.join(workspaceRoot, "STAGES.yaml"),
      [
        "worker: market_analyst",
        "stage: frontline",
        "profiles:",
        "  US:",
        "    approved: false",
        "    prompt: prompts/US.md",
        "    failure: profile blocked for verification",
      ].join("\n"),
    );
    await writeFile(path.join(workspaceRoot, "prompts", "US.md"), "Ticker={ticker}");
    const command = buildCommand("/tmp/evidence");
    command.profile = "US";
    await expect(resolveSingleWorkerProfilePrompt({ workspaceRoot, command })).rejects.toThrowError(
      /not approved/i,
    );
  });
});
