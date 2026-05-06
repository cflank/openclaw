import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { registerOpenVikingTools } from "./openviking-tools.js";
import { createRuntimeContext, type SingleWorkerCommand } from "./params.js";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function readDetailsRecord(result: { details?: unknown }): Record<string, unknown> {
  if (result.details && typeof result.details === "object" && !Array.isArray(result.details)) {
    return result.details as Record<string, unknown>;
  }
  return {};
}

function sha256OfUtf8(content: string): string {
  return crypto.createHash("sha256").update(content, "utf8").digest("hex");
}

function buildCommand(params: {
  evidenceDir: string;
  targetUri: string;
  l2Prefix: string;
}): SingleWorkerCommand {
  return {
    agent: "market_analyst",
    run_id: "run-prestat",
    call_id: "call-1",
    worker_id: "market_analyst",
    stage: "frontline",
    profile: "CN_A",
    runtime_vars: { ticker: "AAPL" },
    allowed_tools: ["openviking.write_material"],
    evidence_dir: params.evidenceDir,
    upstream_materials: [],
    openviking_read_capabilities: [],
    material_target: {
      run_id: "run-prestat",
      call_id: "call-1",
      worker_id: "market_analyst",
      stage: "frontline",
      target_name: "report",
      l1_uri: params.targetUri,
      l2_prefix: params.l2Prefix,
    },
    read_policy: {
      default_layer: "L1",
      allow_l2_when: ["chart_required"],
      forbid_compact_as_writing_source: true,
    },
    stop_after_first_response: false,
  };
}

function buildExpectedEmptyL2IndexContent(command: SingleWorkerCommand): string {
  return `${JSON.stringify(
    {
      entries: [],
      empty_reason: "no_evidence",
      run_id: command.run_id,
      call_id: command.call_id,
      worker_id: command.worker_id,
      stage: command.stage,
    },
    null,
    2,
  )}\n`;
}

type Step = {
  method: string;
  path: string;
  response?: Response;
  error?: Error;
};

function installFetchPlan(steps: Step[], options?: { allowUnusedSteps?: boolean }): () => void {
  const originalFetch = globalThis.fetch;
  const remaining = [...steps];
  const mockFetch: typeof globalThis.fetch = (async (input, init) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const method = (init?.method ?? "GET").toUpperCase();
    const next = remaining.shift();
    if (!next) {
      throw new Error(`unexpected fetch call: ${method} ${url}`);
    }
    expect(method).toBe(next.method.toUpperCase());
    expect(url).toContain(next.path);
    if (next.error) {
      throw next.error;
    }
    if (!next.response) {
      throw new Error(`missing mock response for ${method} ${url}`);
    }
    return next.response;
  }) as typeof globalThis.fetch;
  globalThis.fetch = mockFetch;
  return () => {
    globalThis.fetch = originalFetch;
    if (options?.allowUnusedSteps !== true) {
      expect(remaining).toHaveLength(0);
    }
  };
}

describe("openviking write import-path compat", () => {
  afterEach(async () => {
    await Promise.all(
      tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
  });

  it("writes without fs.stat.before_write and keeps post-write verification", async () => {
    const evidenceDir = await makeTempDir("openviking-import-nostat-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const l2IndexContent = buildExpectedEmptyL2IndexContent(command);
    const l2IndexSize = Buffer.byteLength(l2IndexContent, "utf8");
    const cleanupFetch = installFetchPlan([
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l1" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "report.md",
              size_bytes: 5,
              checksums: {
                sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
              },
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response("hello", { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from("hello", "utf8")), { status: 200 }),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l2" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "index.json",
              size_bytes: l2IndexSize,
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: l2IndexContent,
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from(l2IndexContent, "utf8")), {
          status: 200,
        }),
      },
    ]);
    try {
      const context = createRuntimeContext({ command, openclawRunId: "oc-import-nostat" });
      const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
        (item) => item.name === "openviking.write_material",
      );
      expect(writeTool).toBeDefined();
      const result = await writeTool!.execute("tool-call-import", {
        uri: targetUri,
        content: "hello",
      });
      const details = readDetailsRecord(result);
      expect(details.uri).toBe(targetUri);
      expect(typeof details.receipt_path).toBe("string");
      const receipt = JSON.parse(await fs.readFile(String(details.receipt_path), "utf8")) as Record<
        string,
        unknown
      >;
      const operations = Array.isArray(receipt.http_operations)
        ? (receipt.http_operations as Array<Record<string, unknown>>)
        : [];
      const opNames = operations
        .map((item) => (typeof item.operation === "string" ? item.operation : ""))
        .filter((item) => item.length > 0);
      expect(opNames).not.toContain("fs.stat.before_write");
      expect(opNames).toContain("pack.import");
      expect(opNames).toContain("fs.stat.after_write");
      expect(opNames).toContain("content.read.after_write");
      expect(opNames).toContain("content.download.after_write");
      expect(opNames).toContain("content.download.after_write.l2_index");
    } finally {
      cleanupFetch();
    }
  });

  it("uses content/download bytes for receipt sha256/size when read drops trailing newline", async () => {
    const evidenceDir = await makeTempDir("openviking-import-download-sha-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const l2IndexContent = buildExpectedEmptyL2IndexContent(command);
    const l2IndexSize = Buffer.byteLength(l2IndexContent, "utf8");
    const contentWithTrailingNewline = "hello\n";
    const cleanupFetch = installFetchPlan([
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l1" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "report.md",
              size_bytes: Buffer.byteLength(contentWithTrailingNewline, "utf8"),
              checksums: {
                sha256: sha256OfUtf8(contentWithTrailingNewline),
              },
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response("hello", { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from(contentWithTrailingNewline, "utf8")), {
          status: 200,
        }),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l2" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "index.json",
              size_bytes: l2IndexSize,
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: l2IndexContent,
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from(l2IndexContent, "utf8")), {
          status: 200,
        }),
      },
    ]);
    try {
      const context = createRuntimeContext({ command, openclawRunId: "oc-import-download-sha" });
      const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
        (item) => item.name === "openviking.write_material",
      );
      expect(writeTool).toBeDefined();
      const result = await writeTool!.execute("tool-call-import-download-sha", {
        uri: targetUri,
        content: contentWithTrailingNewline,
      });
      const details = readDetailsRecord(result);
      const receipt = JSON.parse(await fs.readFile(String(details.receipt_path), "utf8")) as Record<
        string,
        unknown
      >;
      expect(details.sha256).toBe(sha256OfUtf8(contentWithTrailingNewline));
      expect(receipt.sha256).toBe(sha256OfUtf8(contentWithTrailingNewline));
      expect(receipt.size_bytes).toBe(Buffer.byteLength(contentWithTrailingNewline, "utf8"));
      const verification = (receipt.verification ?? {}) as Record<string, unknown>;
      expect(verification.expected_sha256).toBe(sha256OfUtf8(contentWithTrailingNewline));
      expect(verification.readback_sha256).toBe(sha256OfUtf8(contentWithTrailingNewline));
      expect(verification.readback_size_bytes).toBe(
        Buffer.byteLength(contentWithTrailingNewline, "utf8"),
      );
      expect(verification.method).toBe(
        "openviking_write_then_stat_then_downloadback_sha_size_identity_check",
      );
    } finally {
      cleanupFetch();
    }
  });

  it("retries l2 index write on transient fetch failure and still returns receipt", async () => {
    const evidenceDir = await makeTempDir("openviking-import-retry-fetch-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const l2IndexContent = buildExpectedEmptyL2IndexContent(command);
    const l2IndexSize = Buffer.byteLength(l2IndexContent, "utf8");
    const cleanupFetch = installFetchPlan([
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l1" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "report.md",
              size_bytes: 5,
              checksums: {
                sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
              },
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response("hello", { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from("hello", "utf8")), { status: 200 }),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        error: new TypeError("fetch failed"),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l2-retry" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "index.json",
              size_bytes: l2IndexSize,
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: l2IndexContent,
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from(l2IndexContent, "utf8")), {
          status: 200,
        }),
      },
    ]);
    try {
      const context = createRuntimeContext({ command, openclawRunId: "oc-import-retry-fetch" });
      const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
        (item) => item.name === "openviking.write_material",
      );
      expect(writeTool).toBeDefined();
      const result = await writeTool!.execute("tool-call-retry-fetch", {
        uri: targetUri,
        content: "hello",
      });
      const details = readDetailsRecord(result);
      const receipt = JSON.parse(await fs.readFile(String(details.receipt_path), "utf8")) as Record<
        string,
        unknown
      >;
      const operations = Array.isArray(receipt.http_operations)
        ? (receipt.http_operations as Array<Record<string, unknown>>)
        : [];
      const retryOps = operations.filter((item) => item.operation === "retry.wait.l2_index");
      expect(retryOps.length).toBeGreaterThan(0);
    } finally {
      cleanupFetch();
    }
  });

  it("retries l2 index write on write EPIPE transport error and still returns receipt", async () => {
    const evidenceDir = await makeTempDir("openviking-import-retry-epipe-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const l2IndexContent = buildExpectedEmptyL2IndexContent(command);
    const l2IndexSize = Buffer.byteLength(l2IndexContent, "utf8");
    const cleanupFetch = installFetchPlan([
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l1" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "report.md",
              size_bytes: 5,
              checksums: {
                sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
              },
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response("hello", { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from("hello", "utf8")), { status: 200 }),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        error: new Error("write EPIPE"),
      },
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l2-retry-epipe" } }),
          { status: 200 },
        ),
      },
      {
        method: "POST",
        path: "/api/v1/pack/import",
        response: new Response(JSON.stringify({ status: "ok", result: {} }), { status: 200 }),
      },
      {
        method: "GET",
        path: "/api/v1/fs/stat?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "index.json",
              size_bytes: l2IndexSize,
            },
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/read?uri=",
        response: new Response(
          JSON.stringify({
            status: "ok",
            result: l2IndexContent,
          }),
          { status: 200 },
        ),
      },
      {
        method: "GET",
        path: "/api/v1/content/download?uri=",
        response: new Response(new Uint8Array(Buffer.from(l2IndexContent, "utf8")), {
          status: 200,
        }),
      },
    ]);
    try {
      const context = createRuntimeContext({ command, openclawRunId: "oc-import-retry-epipe" });
      const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
        (item) => item.name === "openviking.write_material",
      );
      expect(writeTool).toBeDefined();
      const result = await writeTool!.execute("tool-call-retry-epipe", {
        uri: targetUri,
        content: "hello",
      });
      const details = readDetailsRecord(result);
      const receipt = JSON.parse(await fs.readFile(String(details.receipt_path), "utf8")) as Record<
        string,
        unknown
      >;
      const operations = Array.isArray(receipt.http_operations)
        ? (receipt.http_operations as Array<Record<string, unknown>>)
        : [];
      const retryOps = operations.filter((item) => item.operation === "retry.wait.l2_index");
      expect(retryOps.length).toBeGreaterThan(0);
    } finally {
      cleanupFetch();
    }
  });

  it("does not retry non-retryable 4xx business errors", async () => {
    const evidenceDir = await makeTempDir("openviking-import-no-retry-4xx-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const cleanupFetch = installFetchPlan([
      {
        method: "POST",
        path: "/api/v1/resources/temp_upload",
        response: new Response(
          JSON.stringify({
            status: "error",
            error: { code: "BAD_REQUEST", message: "invalid upload payload" },
          }),
          { status: 400 },
        ),
      },
    ]);
    try {
      const context = createRuntimeContext({ command, openclawRunId: "oc-import-no-retry-4xx" });
      const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
        (item) => item.name === "openviking.write_material",
      );
      expect(writeTool).toBeDefined();
      await expect(
        writeTool!.execute("tool-call-no-retry-4xx", { uri: targetUri, content: "hello" }),
      ).rejects.toThrow(/invalid upload payload/i);
    } finally {
      cleanupFetch();
    }
  });

  it("rejects pm_decision for non-portfolio worker before OpenViking HTTP", async () => {
    const evidenceDir = await makeTempDir("openviking-import-pm-decision-scope-");
    const targetUri =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/report.md";
    const l2Prefix =
      "viking://resources/workflow/run-prestat/frontline/market_analyst/call-1/evidence/";
    const command = buildCommand({ evidenceDir, targetUri, l2Prefix });
    const context = createRuntimeContext({ command, openclawRunId: "oc-import-pm-decision-scope" });
    const writeTool = registerOpenVikingTools(context, { baseUrl: "http://127.0.0.1:1933" }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();
    await expect(
      writeTool!.execute("tool-call-pm-scope", {
        uri: targetUri,
        content: "analysis body",
        pm_decision: {
          rating: "buy",
          final_conclusion: "x",
          execution_conditions: ["y"],
          risk_conditions: ["z"],
          source_claim_ids: [],
        },
      }),
    ).rejects.toThrow(/portfolio_manager@portfolio_decision/i);
  });
});
