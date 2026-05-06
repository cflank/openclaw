import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  appendSingleWorkerMaterialBriefToPrompt,
  buildFirstResponseEvidenceFromEvent,
  buildToolCallsEvidencePayload,
  extractToolsFromProviderPayload,
  prependSingleWorkerProfilePromptToPrompt,
  redactSensitiveHeadersForProviderCapture,
  resolveProviderPayloadForCapture,
  writeFirstResponseEvidence,
} from "./attempt.js";
import { createRuntimeContext, type SingleWorkerCommand, withRuntimeMarkers } from "./params.js";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function makeCommand(evidenceDir: string): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    run_id: "run-1",
    call_id: "call-1",
    worker_id: "market_analyst",
    stage: "frontline",
    profile: "CN_A",
    runtime_vars: { ticker: "AAPL" },
    allowed_tools: ["web_search", "openviking.read_with_capability"],
    evidence_dir: evidenceDir,
    upstream_materials: [],
    openviking_read_capabilities: [],
    material_target: {
      run_id: "run-1",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      target_name: "market-analysis",
      l1_uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/l1.md",
      l2_prefix: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/l2/",
    },
    read_policy: {
      default_layer: "L1",
      allow_l2_when: ["chart_required"],
      forbid_compact_as_writing_source: true,
    },
    stop_after_first_response: true,
  };
}

describe("single-worker evidence helpers", () => {
  afterEach(async () => {
    await Promise.all(
      tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
  });

  it("uses final provider payload for capture and visible tools source", async () => {
    const resolved = await resolveProviderPayloadForCapture({
      payload: { tools: [{ name: "old_tool" }] },
      providerModel: { provider: "openai" },
      priorOnPayload: async () => ({ tools: [{ name: "final_tool" }], messages: [] }),
    });
    expect(extractToolsFromProviderPayload(resolved)).toEqual([{ name: "final_tool" }]);
  });

  it("redacts sensitive provider headers while preserving payload tools and runtime markers", () => {
    const context = createRuntimeContext({
      command: makeCommand("/tmp/evidence"),
      openclawRunId: "oc-run-redact",
    });
    const captured = withRuntimeMarkers(
      redactSensitiveHeadersForProviderCapture({
        source: "provider_request_capture",
        provider_model: {
          headers: {
            Authorization: "Bearer sk-live-header-secret",
            "X-API-Key": "token-secret-value",
            "OpenAI-Api-Key": "openai-secret-value",
            "X-Trace-Id": "trace-123",
          },
        },
        payload: {
          messages: [{ role: "user", content: "hello" }],
          tools: [{ name: "openvikingArtifact__ov_mkt__stock_price" }],
          headers: {
            "Proxy-Authorization": "Bearer sk-proxy-secret",
            "api-key": "payload-api-key-secret",
            "X-Debug": "visible",
          },
        },
      }),
      context,
    ) as Record<string, unknown>;

    const serialized = JSON.stringify(captured);
    expect(serialized).not.toContain("Bearer ");
    expect(serialized).not.toContain("sk-live-header-secret");
    expect(serialized).not.toContain("token-secret-value");
    expect(serialized).toContain('"Authorization":"<redacted>"');
    expect(serialized).toContain('"X-API-Key":"<redacted>"');
    expect(serialized).toContain('"OpenAI-Api-Key":"<redacted>"');
    expect(serialized).toContain('"Proxy-Authorization":"<redacted>"');
    expect(serialized).toContain('"api-key":"<redacted>"');
    expect(serialized).toContain('"X-Trace-Id":"trace-123"');
    expect(serialized).toContain('"X-Debug":"visible"');
    expect(serialized).toContain('"tools":[{"name":"openvikingArtifact__ov_mkt__stock_price"}]');
    expect(serialized).toContain('"runtime_markers":{');
    expect(serialized).toContain('"run_id":"run-1"');
    expect(serialized).toContain('"call_id":"call-1"');
    expect(serialized).toContain('"worker_id":"market_analyst"');
    expect(serialized).toContain('"stage":"frontline"');
  });

  it("preserves material brief in final payload when prompt submission appends upstream materials", async () => {
    const context = createRuntimeContext({
      command: {
        ...makeCommand("/tmp/evidence"),
        upstream_materials: [
          {
            material_id: "mat-1",
            capability_id: "cap-1",
            worker_id: "market_analyst",
            stage: "frontline",
            l1_uri: "viking://resources/workflow/run-0/frontline/market_analyst/call-0/report.md",
            l1_sha256: "abc123",
            l2_index_uri:
              "viking://resources/workflow/run-0/frontline/market_analyst/call-0/evidence/index.json",
            l2_allowed_prefix:
              "viking://resources/workflow/run-0/frontline/market_analyst/call-0/evidence/",
            call_id: "call-0",
          },
        ],
      },
      openclawRunId: "oc-run-brief",
    });
    const promptWithProfile = prependSingleWorkerProfilePromptToPrompt(
      "Complete this worker turn using configured workspace instructions.",
      "Ticker=AAPL Company=Apple Currency=USD Date=2026-05-03",
    );
    const promptWithBrief = appendSingleWorkerMaterialBriefToPrompt(promptWithProfile, context);
    expect(promptWithBrief).toContain("[OpenVikingReadableMaterials]");
    const resolved = await resolveProviderPayloadForCapture({
      payload: { messages: [{ role: "user", content: "old prompt" }] },
      providerModel: { provider: "openai" },
      priorOnPayload: async () => ({
        messages: [{ role: "user", content: promptWithBrief }],
      }),
    });
    const capturedMessages =
      (resolved as { messages?: Array<{ content?: string }> }).messages ?? [];
    expect(capturedMessages[0]?.content).toContain("Ticker=AAPL");
    expect(capturedMessages[0]?.content).not.toContain("{ticker}");
    expect(capturedMessages[0]?.content).toContain("[OpenVikingReadableMaterials]");
    expect(capturedMessages[0]?.content).toContain("material_id=mat-1");
    expect(capturedMessages[0]?.content).not.toContain("上游正文");
  });

  it("builds first response evidence with openclaw source and event_kind", () => {
    const context = createRuntimeContext({
      command: makeCommand("/tmp/evidence"),
      openclawRunId: "oc-run-1",
    });
    const evidence = buildFirstResponseEvidenceFromEvent({
      event: {
        stream: "tool",
        data: { phase: "start", toolCallId: "tool-1", name: "openviking.read_with_capability" },
      },
      markers: context.markers,
      capturedAt: "2026-05-04T00:00:00.000Z",
    });
    expect(evidence).toMatchObject({
      source: "openclaw_first_model_event",
      event_kind: "tool_call",
      tool_call: {
        tool_call_id: "tool-1",
        name: "openviking.read_with_capability",
      },
    });
  });

  it("writes first-response json+txt and awaits durable path", async () => {
    const root = await makeTempDir("openclaw-first-response-");
    const evidenceDir = path.join(root, "evidence");
    const context = createRuntimeContext({
      command: makeCommand(evidenceDir),
      openclawRunId: "oc-run-2",
    });
    const firstResponsePath = await writeFirstResponseEvidence({
      context,
      evidence: {
        source: "openclaw_first_model_event",
        run_id: "run-1",
        call_id: "call-1",
        worker_id: "market_analyst",
        stage: "frontline",
        profile: "CN_A",
        openclaw_run_id: "oc-run-2",
        event_kind: "assistant_text",
        text: "先调用工具，再给结论。",
        captured_at: "2026-05-04T00:00:00.000Z",
      },
    });
    const evidenceJson = JSON.parse(await fs.readFile(firstResponsePath, "utf8")) as Record<
      string,
      unknown
    >;
    const textPath = String(evidenceJson.text_path);
    const textBody = await fs.readFile(textPath, "utf8");
    expect(evidenceJson.event_kind).toBe("assistant_text");
    expect(textBody).toBe("先调用工具，再给结论。");
  });

  it("builds tool-calls payload with top-level calls field", () => {
    const payload = buildToolCallsEvidencePayload({
      status: "recorded",
      calls: [
        {
          tool_call_id: "tool-1",
          tool_name: "openviking.read_with_capability",
          action: "read",
          capability_id: "cap-1",
          material_id: "mat-1",
          uri: "viking://resources/workflow/run-1/frontline/market_analyst/call-1/l1.md",
          result_sha256: "abc",
          status: "success",
          started_at: "2026-05-04T00:00:00.000Z",
          finished_at: "2026-05-04T00:00:01.000Z",
        },
      ],
      markers: {
        run_id: "run-1",
        call_id: "call-1",
        worker_id: "market_analyst",
        stage: "frontline",
        profile: "CN_A",
        openclaw_run_id: "oc-run-3",
      },
    });
    expect(payload.source).toBe("model_tool_events");
    expect(payload.run_id).toBe("run-1");
    expect(payload.call_id).toBe("call-1");
    expect(payload.worker_id).toBe("market_analyst");
    expect(payload.stage).toBe("frontline");
    expect(payload.profile).toBe("CN_A");
    expect(payload.openclaw_run_id).toBe("oc-run-3");
    expect(payload.status).toBe("recorded");
    expect(Array.isArray(payload.calls)).toBe(true);
    expect(payload.calls[0]?.tool_name).toBe("openviking.read_with_capability");
  });
});
