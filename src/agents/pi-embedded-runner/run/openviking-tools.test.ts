import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import JSZip from "jszip";
import { afterEach, describe, expect, it, vi } from "vitest";
import { registerOpenVikingTools } from "./openviking-tools.js";
import { createRuntimeContext, type SingleWorkerCommand } from "./params.js";

const OPENVIKING_BASE_URL = "http://127.0.0.1:1933";
const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function sha256OfUtf8(content: string): string {
  return crypto.createHash("sha256").update(content, "utf8").digest("hex");
}

function normalizeOpenVikingText(content: string): string {
  return content.replace(/\r\n/g, "\n").replace(/\n+$/g, "");
}

function sha256OfBytes(content: Uint8Array): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

function readDetailsRecord(result: { details?: unknown }): Record<string, unknown> {
  if (result.details && typeof result.details === "object" && !Array.isArray(result.details)) {
    return result.details as Record<string, unknown>;
  }
  return {};
}

async function requestOpenViking(
  endpoint: string,
  init?: RequestInit,
): Promise<Record<string, unknown> | string> {
  const response = await fetch(`${OPENVIKING_BASE_URL}${endpoint}`, init);
  const bodyText = await response.text();
  let parsed: unknown = bodyText;
  if (bodyText.trim().length > 0) {
    try {
      parsed = JSON.parse(bodyText);
    } catch {
      parsed = bodyText;
    }
  }
  if (!response.ok) {
    throw new Error(`OpenViking request failed: ${endpoint} ${response.status} ${bodyText}`);
  }
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    const record = parsed as Record<string, unknown>;
    if (record.status === "error") {
      throw new Error(`OpenViking request error: ${endpoint} ${JSON.stringify(record)}`);
    }
    if (Object.prototype.hasOwnProperty.call(record, "result")) {
      const result = record.result;
      return result && typeof result === "object" && !Array.isArray(result)
        ? (result as Record<string, unknown>)
        : (result as string);
    }
    return record;
  }
  return typeof parsed === "string" ? parsed : JSON.stringify(parsed);
}

async function downloadOpenVikingBytes(endpoint: string): Promise<Uint8Array> {
  const response = await fetch(`${OPENVIKING_BASE_URL}${endpoint}`);
  if (!response.ok) {
    const bodyText = await response.text();
    throw new Error(`OpenViking download failed: ${endpoint} ${response.status} ${bodyText}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

async function putContentViaImport(uri: string, content: string): Promise<void> {
  const normalizedUri = uri.replace(/\/+$/, "");
  const slash = normalizedUri.lastIndexOf("/");
  if (slash < "viking://".length + 1) {
    throw new Error(`invalid uri for putContentViaImport: ${uri}`);
  }
  const parent = normalizedUri.slice(0, slash);
  const fileName = normalizedUri.slice(slash + 1);
  const zip = new JSZip();
  zip.file(fileName, content);
  const zipBuffer = await zip.generateAsync({ type: "nodebuffer" });
  const form = new FormData();
  form.append(
    "file",
    new Blob([Uint8Array.from(zipBuffer)], { type: "application/zip" }),
    `${fileName}.zip`,
  );
  const uploadResult = await requestOpenViking("/api/v1/resources/temp_upload", {
    method: "POST",
    body: form,
  });
  const tempFileId =
    typeof uploadResult === "object" && uploadResult
      ? String(uploadResult.temp_file_id ?? "").trim()
      : "";
  if (!tempFileId) {
    throw new Error("OpenViking temp upload missing temp_file_id");
  }
  await requestOpenViking("/api/v1/pack/import", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      temp_file_id: tempFileId,
      parent,
      force: true,
      vectorize: false,
    }),
  });
}

function buildCommand(params: {
  evidenceDir: string;
  targetUri: string;
  l2Prefix: string;
  runId?: string;
  callId?: string;
  workerId?: string;
  stage?: string;
  capabilities?: SingleWorkerCommand["openviking_read_capabilities"];
  upstreamMaterials?: SingleWorkerCommand["upstream_materials"];
}): SingleWorkerCommand {
  const runId = params.runId ?? "run-t34a";
  const callId = params.callId ?? "call-1";
  const workerId = params.workerId ?? "market_analyst";
  const stage = params.stage ?? "frontline";
  return {
    agent: "market_analyst",
    run_id: runId,
    call_id: callId,
    worker_id: workerId,
    stage,
    profile: "CN_A",
    runtime_vars: {
      ticker: "AAPL",
    },
    allowed_tools: ["openviking.write_material", "openviking.read_with_capability"],
    evidence_dir: params.evidenceDir,
    upstream_materials: params.upstreamMaterials ?? [],
    openviking_read_capabilities: params.capabilities ?? [],
    material_target: {
      run_id: runId,
      call_id: callId,
      worker_id: workerId,
      stage,
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

function buildExpectedMaterialId(params: {
  runId: string;
  callId: string;
  workerId: string;
  stage: string;
  targetName: string;
  uri: string;
  sha256: string;
}): string {
  return `mat-${crypto
    .createHash("sha256")
    .update(
      [
        params.runId,
        params.callId,
        params.workerId,
        params.stage,
        params.targetName,
        params.uri,
        params.sha256,
      ].join("|"),
      "utf8",
    )
    .digest("hex")
    .slice(0, 24)}`;
}

describe("openviking tools integration", () => {
  afterEach(async () => {
    await Promise.all(
      tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
  });

  it("writes material via real OpenViking HTTP and reads it back with capability", async () => {
    const health = await fetch(`${OPENVIKING_BASE_URL}/health`);
    expect(health.ok).toBe(true);

    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-evidence-");
    const content = `# T34A focused\n\ntimestamp=${stamp}\n`;
    const normalizedContent = normalizeOpenVikingText(content);

    const writeCommand = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
    });
    const writeContext = createRuntimeContext({
      command: writeCommand,
      openclawRunId: `openclaw-write-${stamp}`,
    });
    const writeTools = registerOpenVikingTools(writeContext, { baseUrl: OPENVIKING_BASE_URL });
    const writeTool = writeTools.find((item) => item.name === "openviking.write_material");
    expect(writeTool).toBeDefined();

    const writeResult = await writeTool!.execute("call-write-1", {
      uri: targetUri,
      content,
    });
    const expectedDownloadBytes = await downloadOpenVikingBytes(
      `/api/v1/content/download?uri=${encodeURIComponent(targetUri)}`,
    );
    const expectedDownloadSha = sha256OfBytes(expectedDownloadBytes);
    const expectedDownloadSize = expectedDownloadBytes.byteLength;
    const writeDetails = readDetailsRecord(writeResult);
    expect(writeDetails.uri).toBe(targetUri);
    expect(typeof writeDetails.receipt_path).toBe("string");
    expect(typeof writeDetails.claims_path).toBe("string");
    expect(typeof writeDetails.sha256).toBe("string");
    const receiptPath = String(writeDetails.receipt_path);
    await expect(fs.access(receiptPath)).resolves.toBeUndefined();
    const claimsPath = String(writeDetails.claims_path);
    await expect(fs.access(claimsPath)).resolves.toBeUndefined();
    const receipt = JSON.parse(await fs.readFile(receiptPath, "utf8")) as Record<string, unknown>;
    const claims = JSON.parse(await fs.readFile(claimsPath, "utf8")) as Record<string, unknown>;
    expect(receipt.source).toBe("openviking_adapter_verified_receipt");
    expect(typeof receipt.receipt_id).toBe("string");
    expect(String(receipt.receipt_id).trim().length).toBeGreaterThan(0);
    expect(receipt.receipt_label).toBe("verified_openviking_write_receipt");
    expect(receipt.receipt_origin).toBe("adapter_verified_non_native");
    expect(receipt.is_openviking_native_receipt).toBe(false);
    expect(receipt.uri).toBe(targetUri);
    expect(receipt.sha256).toBe(writeDetails.sha256);
    expect(receipt.run_id).toBe(writeCommand.run_id);
    expect(receipt.stage).toBe(writeCommand.stage);
    expect(receipt.worker_id).toBe(writeCommand.worker_id);
    expect(receipt.call_id).toBe(writeCommand.call_id);
    expect(receipt.target_name).toBe("report");
    const verification = (receipt.verification ?? {}) as Record<string, unknown>;
    expect(verification.verified).toBe(true);
    expect(verification.method).toBe(
      "openviking_write_then_stat_then_downloadback_sha_size_identity_check",
    );
    const operations = Array.isArray(receipt.http_operations)
      ? (receipt.http_operations as Array<Record<string, unknown>>)
      : [];
    const operationNames = operations
      .map((item) => (typeof item.operation === "string" ? item.operation : ""))
      .filter((item) => item.length > 0);
    expect(operationNames).toContain("fs.stat.after_write");
    expect(operationNames).toContain("content.read.after_write");
    expect(operationNames).toContain("content.download.after_write");
    expect(operationNames).toContain("fs.stat.after_write.l2_index");
    expect(operationNames).toContain("content.read.after_write.l2_index");
    expect(operationNames).toContain("content.download.after_write.l2_index");
    expect(
      operationNames.includes("content.write.replace") || operationNames.includes("pack.import"),
    ).toBe(true);
    expect(claims.schema_version).toBe("control.claims.v1");
    expect(claims.source).toBe("openclaw_openviking_write_material");
    expect(claims.run_id).toBe(writeCommand.run_id);
    expect(claims.call_id).toBe(writeCommand.call_id);
    expect(claims.worker_id).toBe(writeCommand.worker_id);
    expect(claims.stage).toBe(writeCommand.stage);
    expect(claims.target_name).toBe(writeCommand.material_target.target_name);
    expect(claims.l1_uri).toBe(targetUri);
    expect(claims.l1_sha256).toBe(writeDetails.sha256);
    expect(claims.l1_sha256).toBe(expectedDownloadSha);
    expect(claims.material_layer).toBe("L1");
    expect(claims.source_kind).toBe("worker_report");
    expect(claims.claims).toEqual([]);
    expect(claims.material_id).toBe(
      buildExpectedMaterialId({
        runId: writeCommand.run_id,
        callId: writeCommand.call_id,
        workerId: writeCommand.worker_id,
        stage: writeCommand.stage,
        targetName: writeCommand.material_target.target_name,
        uri: targetUri,
        sha256: String(writeDetails.sha256),
      }),
    );

    const l2IndexUri = `${l2Prefix}index.json`;
    const l2IndexRaw = await requestOpenViking(
      `/api/v1/content/read?uri=${encodeURIComponent(l2IndexUri)}`,
    );
    expect(typeof l2IndexRaw).toBe("string");
    const l2Index = JSON.parse(String(l2IndexRaw)) as Record<string, unknown>;
    expect(Array.isArray(l2Index.entries)).toBe(true);
    expect((l2Index.entries as unknown[]).length).toBe(0);
    expect(l2Index.empty_reason).toBe("no_evidence");

    const capability = {
      capability_id: `cap-${stamp}`,
      material_id: `mat-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: String(writeDetails.sha256),
      allowed_l2_prefix: l2Prefix,
      manifest_entry_sha256: sha256OfUtf8(`manifest-${stamp}`),
    };
    const readCommand = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
    });
    const readContext = createRuntimeContext({
      command: readCommand,
      openclawRunId: `openclaw-read-${stamp}`,
    });
    const readTools = registerOpenVikingTools(readContext, { baseUrl: OPENVIKING_BASE_URL });
    const readTool = readTools.find((item) => item.name === "openviking.read_with_capability");
    expect(readTool).toBeDefined();

    const readResult = await readTool!.execute("call-read-1", {
      material_id: capability.material_id,
      layer: "L1",
    });
    const readDetails = readDetailsRecord(readResult);
    expect(readDetails.content).toBe(normalizedContent);
    expect(readDetails.sha256).toBe(expectedDownloadSha);
    expect(readDetails.sha256).toBe(writeDetails.sha256);
    expect(Number(verification.expected_size_bytes)).toBe(expectedDownloadSize);
    expect(readDetails.uri).toBe(targetUri);
    expect(readDetails.material_id).toBe(capability.material_id);
    expect(readDetails.capability_id).toBe(capability.capability_id);
  });

  it("rejects read URI outside capability scope", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-evidence-deny-");
    const capability = {
      capability_id: `cap-${stamp}`,
      material_id: `mat-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: sha256OfUtf8("dummy"),
      allowed_l2_prefix: l2Prefix,
      manifest_entry_sha256: sha256OfUtf8(`manifest-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-deny-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();

    await expect(
      readTool!.execute("call-read-deny", {
        capability_id: capability.capability_id,
        material_id: capability.material_id,
        uri: "viking://resources/workflow/unauthorized/frontline/market_analyst/call-1/report.md",
      }),
    ).rejects.toThrow(/capability/i);
  });

  it("supports material_id + uri read without capability_id", async () => {
    const health = await fetch(`${OPENVIKING_BASE_URL}/health`);
    expect(health.ok).toBe(true);
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-read-uri-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-read-uri-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-read-uri-");
    const content = `read uri test ${stamp}\n`;
    await putContentViaImport(targetUri, content);
    const normalized = normalizeOpenVikingText(content);
    const capability = {
      capability_id: `cap-${stamp}`,
      material_id: `mat-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: sha256OfUtf8(content),
      allowed_l2_prefix: l2Prefix,
      manifest_entry_sha256: sha256OfUtf8(`manifest-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-read-uri-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();
    const result = await readTool!.execute("call-read-uri", {
      material_id: capability.material_id,
      uri: targetUri,
    });
    const details = readDetailsRecord(result);
    expect(details.material_id).toBe(capability.material_id);
    expect(details.capability_id).toBe(capability.capability_id);
    expect(details.layer).toBe("L1");
    expect(details.content).toBe(normalized);
  });

  it("rejects write content when content includes reserved claims marker", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-reject-claims-marker-");
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-reject-claims-marker-${stamp}`,
    });
    const writeTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();
    await expect(
      writeTool!.execute("call-write-reject", {
        uri: targetUri,
        content: '```json\n{"schema_version":"control.claims.v1"}\n```',
      }),
    ).rejects.toThrow(/control\.claims\.v1/i);
  });

  it("writes pm-decision evidence only for portfolio_manager@portfolio_decision", async () => {
    const health = await fetch(`${OPENVIKING_BASE_URL}/health`);
    expect(health.ok).toBe(true);
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-pm-${stamp}/portfolio_decision/portfolio_manager/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-pm-${stamp}/portfolio_decision/portfolio_manager/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-pm-decision-");
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      workerId: "portfolio_manager",
      stage: "portfolio_decision",
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-pm-decision-${stamp}`,
    });
    const writeTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();
    const result = await writeTool!.execute("call-write-pm", {
      uri: targetUri,
      content: "PM report body",
      pm_decision: {
        rating: "buy",
        final_conclusion: "maintain long bias",
        execution_conditions: ["breakout above resistance"],
        risk_conditions: ["stop below support"],
        source_claim_ids: [],
      },
    });
    const details = readDetailsRecord(result);
    expect(typeof details.pm_decision_path).toBe("string");
    expect(typeof details.claims_path).toBe("string");
    const pmDecisionPath = String(details.pm_decision_path);
    await expect(fs.access(pmDecisionPath)).resolves.toBeUndefined();
    const pmDecision = JSON.parse(await fs.readFile(pmDecisionPath, "utf8")) as Record<
      string,
      unknown
    >;
    const expectedTopLevelFields = [
      "schema_version",
      "run_id",
      "call_id",
      "worker_id",
      "stage",
      "material_id",
      "rating",
      "final_conclusion",
      "execution_conditions",
      "risk_conditions",
      "source_claim_ids",
      "source_l1_sha256",
      "l1_uri",
    ];
    for (const field of expectedTopLevelFields) {
      expect(Object.prototype.hasOwnProperty.call(pmDecision, field)).toBe(true);
    }
    expect(Object.prototype.hasOwnProperty.call(pmDecision, "pm_decision")).toBe(false);
    expect(pmDecision.schema_version).toBe("control.pm_decision.v1");
    expect(pmDecision.run_id).toBe(command.run_id);
    expect(pmDecision.call_id).toBe(command.call_id);
    expect(pmDecision.worker_id).toBe("portfolio_manager");
    expect(pmDecision.stage).toBe("portfolio_decision");
    expect(pmDecision.l1_uri).toBe(targetUri);
    expect(pmDecision.rating).toBe("buy");
    expect(pmDecision.final_conclusion).toBe("maintain long bias");
    expect(pmDecision.execution_conditions).toEqual(["breakout above resistance"]);
    expect(pmDecision.risk_conditions).toEqual(["stop below support"]);
    expect(pmDecision.source_claim_ids).toEqual([]);
  });

  it("rejects pm_decision for non-portfolio worker/stage", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-pm-reject-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-pm-reject-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-pm-decision-reject-");
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      workerId: "market_analyst",
      stage: "frontline",
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-pm-decision-reject-${stamp}`,
    });
    const writeTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();
    await expect(
      writeTool!.execute("call-write-pm-reject", {
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

  it("rejects pm_decision when rating is outside allowed enum and does not write pm-decision file", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-pm-rating-${stamp}/portfolio_decision/portfolio_manager/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-pm-rating-${stamp}/portfolio_decision/portfolio_manager/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-pm-rating-reject-");
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      workerId: "portfolio_manager",
      stage: "portfolio_decision",
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-pm-rating-reject-${stamp}`,
    });
    const writeTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();
    await expect(
      writeTool!.execute("call-write-pm-rating-reject", {
        uri: targetUri,
        content: "PM report body",
        pm_decision: {
          rating: "HOLD / OBSERVATION-ONLY",
          final_conclusion: "wait for confirmation",
          execution_conditions: ["volume expansion"],
          risk_conditions: ["break below support"],
          source_claim_ids: [],
        },
      }),
    ).rejects.toThrow(/pm_decision\.rating must be one of: buy, hold, sell, neutral, not_rated/i);

    await expect(fs.access(path.join(evidenceDir, "pm-decision.json"))).rejects.toThrow();
  });

  it("binds L2 reads to L2 index sha and entry sha", async () => {
    const health = await fetch(`${OPENVIKING_BASE_URL}/health`);
    expect(health.ok).toBe(true);

    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-l2-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-l2-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const l2Uri = `${l2Prefix}evidence-1.json`;
    const l2IndexUri = `${l2Prefix}index.json`;
    const evidenceDir = await makeTempDir("openclaw-openviking-l2-");

    const l2Content = `{\"kind\":\"raw\",\"stamp\":\"${stamp}\"}\n`;
    await putContentViaImport(l2Uri, l2Content);
    const l2Normalized = normalizeOpenVikingText(l2Content);
    const l2Sha = sha256OfUtf8(l2Content);

    const l2IndexContent = `${JSON.stringify(
      {
        entries: [{ uri: l2Uri, sha256: l2Sha }],
      },
      null,
      2,
    )}\n`;
    await putContentViaImport(l2IndexUri, l2IndexContent);
    const l2IndexSha = sha256OfUtf8(l2IndexContent);

    const capability = {
      capability_id: `cap-l2-${stamp}`,
      material_id: `mat-l2-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: sha256OfUtf8("dummy"),
      allowed_l2_prefix: l2Prefix,
      allowed_l2_index_sha256: l2IndexSha,
      manifest_entry_sha256: sha256OfUtf8(`manifest-l2-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
      upstreamMaterials: [
        {
          material_id: capability.material_id,
          capability_id: capability.capability_id,
          worker_id: "market_analyst",
          stage: "frontline",
          l1_uri: targetUri,
          l1_sha256: capability.allowed_l1_sha256,
          l2_index_uri: l2IndexUri,
          l2_allowed_prefix: l2Prefix,
          call_id: "call-upstream-1",
        },
      ],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-l2-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();

    const readResult = await readTool!.execute("call-read-l2", {
      capability_id: capability.capability_id,
      material_id: capability.material_id,
      uri: l2Uri,
    });
    const readDetails = readDetailsRecord(readResult);
    expect(readDetails.uri).toBe(l2Uri);
    expect(readDetails.sha256).toBe(l2Sha);
    expect(readDetails.content).toBe(l2Normalized);
  });

  it("rejects L2 read when l2 index sha does not match capability", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-l2-sha-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-l2-sha-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const l2Uri = `${l2Prefix}evidence-1.json`;
    const l2IndexUri = `${l2Prefix}index.json`;
    const evidenceDir = await makeTempDir("openclaw-openviking-l2-sha-");

    const l2Content = `{\"kind\":\"raw\",\"stamp\":\"${stamp}\"}\n`;
    await putContentViaImport(l2Uri, l2Content);
    const l2Sha = sha256OfUtf8(l2Content);
    const l2IndexContent = `${JSON.stringify({ entries: [{ uri: l2Uri, sha256: l2Sha }] })}\n`;
    await putContentViaImport(l2IndexUri, l2IndexContent);

    const capability = {
      capability_id: `cap-l2-${stamp}`,
      material_id: `mat-l2-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: sha256OfUtf8("dummy"),
      allowed_l2_prefix: l2Prefix,
      allowed_l2_index_sha256: sha256OfUtf8("wrong-index-sha"),
      manifest_entry_sha256: sha256OfUtf8(`manifest-l2-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
      upstreamMaterials: [
        {
          material_id: capability.material_id,
          capability_id: capability.capability_id,
          worker_id: "market_analyst",
          stage: "frontline",
          l1_uri: targetUri,
          l1_sha256: capability.allowed_l1_sha256,
          l2_index_uri: l2IndexUri,
          l2_allowed_prefix: l2Prefix,
          call_id: "call-upstream-2",
        },
      ],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-l2-sha-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();

    await expect(
      readTool!.execute("call-read-l2-fail", {
        capability_id: capability.capability_id,
        material_id: capability.material_id,
        uri: l2Uri,
      }),
    ).rejects.toThrow(/index sha256 mismatch/i);
  });

  it("rejects L2 read when index entry sha256 does not match actual content sha256", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t34a-l2-entry-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t34a-l2-entry-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const l2Uri = `${l2Prefix}evidence-1.json`;
    const l2IndexUri = `${l2Prefix}index.json`;
    const evidenceDir = await makeTempDir("openclaw-openviking-l2-entry-sha-");

    const l2Content = `{\"kind\":\"raw\",\"stamp\":\"${stamp}\"}\n`;
    await putContentViaImport(l2Uri, l2Content);
    const wrongEntrySha = sha256OfUtf8(`wrong-entry-${stamp}`);
    const l2IndexContent = `${JSON.stringify({ entries: [{ uri: l2Uri, sha256: wrongEntrySha }] })}\n`;
    await putContentViaImport(l2IndexUri, l2IndexContent);
    const l2IndexSha = sha256OfUtf8(l2IndexContent);

    const capability = {
      capability_id: `cap-l2-entry-${stamp}`,
      material_id: `mat-l2-entry-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: sha256OfUtf8("dummy"),
      allowed_l2_prefix: l2Prefix,
      allowed_l2_index_sha256: l2IndexSha,
      manifest_entry_sha256: sha256OfUtf8(`manifest-l2-entry-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
      upstreamMaterials: [
        {
          material_id: capability.material_id,
          capability_id: capability.capability_id,
          worker_id: "market_analyst",
          stage: "frontline",
          l1_uri: targetUri,
          l1_sha256: capability.allowed_l1_sha256,
          l2_index_uri: l2IndexUri,
          l2_allowed_prefix: l2Prefix,
          call_id: "call-upstream-3",
        },
      ],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-l2-entry-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();

    await expect(
      readTool!.execute("call-read-l2-entry-fail", {
        capability_id: capability.capability_id,
        material_id: capability.material_id,
        uri: l2Uri,
      }),
    ).rejects.toThrow(/L2 content sha256 mismatch/i);
  });
});

describe("openviking tools canonical download bytes", () => {
  afterEach(async () => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    await Promise.all(
      tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
  });

  it("uses content/download canonical bytes for receipt, claims, and PM sidecar sha", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t62-canonical-${stamp}/portfolio_decision/portfolio_manager/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t62-canonical-${stamp}/portfolio_decision/portfolio_manager/call-1/evidence/`;
    const evidenceDir = await makeTempDir("openclaw-openviking-canonical-write-");
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      workerId: "portfolio_manager",
      stage: "portfolio_decision",
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-canonical-write-${stamp}`,
    });
    const writeTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.write_material",
    );
    expect(writeTool).toBeDefined();

    const l1Canonical = "canonical-content-with-trailing-newline\n";
    const l1Display = "canonical-content-with-trailing-newline";
    const l1CanonicalBytes = Uint8Array.from(Buffer.from(l1Canonical, "utf8"));
    const l1CanonicalSha = sha256OfBytes(l1CanonicalBytes);
    const l1DisplaySha = sha256OfUtf8(l1Display);
    expect(l1CanonicalSha).not.toBe(l1DisplaySha);

    const emptyL2IndexCanonical = `${JSON.stringify(
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
    const emptyL2IndexDisplay = emptyL2IndexCanonical.replace(/\n$/, "");
    const emptyL2IndexBytes = Uint8Array.from(Buffer.from(emptyL2IndexCanonical, "utf8"));
    const emptyL2IndexSha = sha256OfBytes(emptyL2IndexBytes);

    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    fetchMock
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l1" } }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ status: "ok", result: {} }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "report.md",
              size_bytes: l1CanonicalBytes.byteLength,
              sha256: l1CanonicalSha,
            },
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        ),
      )
      .mockResolvedValueOnce(new Response(l1Display, { status: 200 }))
      .mockResolvedValueOnce(
        new Response(l1CanonicalBytes, {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ status: "ok", result: { temp_file_id: "tmp-l2-index" } }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ status: "ok", result: {} }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            status: "ok",
            result: {
              name: "index.json",
              size_bytes: emptyL2IndexBytes.byteLength,
              sha256: emptyL2IndexSha,
            },
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        ),
      )
      .mockResolvedValueOnce(new Response(emptyL2IndexDisplay, { status: 200 }))
      .mockResolvedValueOnce(
        new Response(emptyL2IndexBytes, {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      );

    const writeResult = await writeTool!.execute("call-write-canonical", {
      uri: targetUri,
      content: l1Canonical,
      pm_decision: {
        rating: "buy",
        final_conclusion: "preserve canonical bytes sha",
        execution_conditions: ["confirm breakout"],
        risk_conditions: ["respect stop"],
        source_claim_ids: [],
      },
    });
    const writeDetails = readDetailsRecord(writeResult);
    expect(writeDetails.sha256).toBe(l1CanonicalSha);
    expect(writeDetails.sha256).not.toBe(l1DisplaySha);

    const receiptPath = String(writeDetails.receipt_path);
    const claimsPath = String(writeDetails.claims_path);
    const pmDecisionPath = String(writeDetails.pm_decision_path);
    const receipt = JSON.parse(await fs.readFile(receiptPath, "utf8")) as Record<string, unknown>;
    const verification = (receipt.verification ?? {}) as Record<string, unknown>;
    const claims = JSON.parse(await fs.readFile(claimsPath, "utf8")) as Record<string, unknown>;
    const pmDecision = JSON.parse(await fs.readFile(pmDecisionPath, "utf8")) as Record<
      string,
      unknown
    >;

    expect(receipt.sha256).toBe(l1CanonicalSha);
    expect(receipt.size_bytes).toBe(l1CanonicalBytes.byteLength);
    expect(verification.expected_sha256).toBe(l1CanonicalSha);
    expect(verification.readback_sha256).toBe(l1CanonicalSha);
    expect(verification.expected_size_bytes).toBe(l1CanonicalBytes.byteLength);
    expect(verification.readback_size_bytes).toBe(l1CanonicalBytes.byteLength);
    expect(claims.l1_sha256).toBe(l1CanonicalSha);
    expect(pmDecision.source_l1_sha256).toBe(l1CanonicalSha);
  });

  it("uses content/download canonical bytes for read_with_capability L1/L2 result sha256", async () => {
    const stamp = `${Date.now()}`;
    const targetUri = `viking://resources/workflow/test-t62-read-canonical-${stamp}/frontline/market_analyst/call-1/report.md`;
    const l2Prefix = `viking://resources/workflow/test-t62-read-canonical-${stamp}/frontline/market_analyst/call-1/evidence/`;
    const l2Uri = `${l2Prefix}evidence-1.json`;
    const l2IndexUri = `${l2Prefix}index.json`;
    const evidenceDir = await makeTempDir("openclaw-openviking-canonical-read-");

    const l1Canonical = "l1-with-newline\n";
    const l1Display = "l1-with-newline";
    const l1CanonicalBytes = Uint8Array.from(Buffer.from(l1Canonical, "utf8"));
    const l1CanonicalSha = sha256OfBytes(l1CanonicalBytes);
    const l1DisplaySha = sha256OfUtf8(l1Display);
    expect(l1CanonicalSha).not.toBe(l1DisplaySha);

    const l2Canonical = '{"kind":"raw"}\n';
    const l2Display = '{"kind":"raw"}';
    const l2CanonicalBytes = Uint8Array.from(Buffer.from(l2Canonical, "utf8"));
    const l2CanonicalSha = sha256OfBytes(l2CanonicalBytes);
    const l2DisplaySha = sha256OfUtf8(l2Display);
    expect(l2CanonicalSha).not.toBe(l2DisplaySha);

    const l2IndexCanonical = `${JSON.stringify({ entries: [{ uri: l2Uri, sha256: l2CanonicalSha }] })}\n`;
    const l2IndexDisplay = l2IndexCanonical.replace(/\n$/, "");
    const l2IndexBytes = Uint8Array.from(Buffer.from(l2IndexCanonical, "utf8"));
    const l2IndexSha = sha256OfBytes(l2IndexBytes);

    const capability = {
      capability_id: `cap-read-${stamp}`,
      material_id: `mat-read-${stamp}`,
      allowed_l1_uri: targetUri,
      allowed_l1_sha256: l1CanonicalSha,
      allowed_l2_prefix: l2Prefix,
      allowed_l2_index_sha256: l2IndexSha,
      manifest_entry_sha256: sha256OfUtf8(`manifest-read-${stamp}`),
    };
    const command = buildCommand({
      evidenceDir,
      targetUri,
      l2Prefix,
      capabilities: [capability],
      upstreamMaterials: [
        {
          material_id: capability.material_id,
          capability_id: capability.capability_id,
          worker_id: "market_analyst",
          stage: "frontline",
          l1_uri: targetUri,
          l1_sha256: capability.allowed_l1_sha256,
          l2_index_uri: l2IndexUri,
          l2_allowed_prefix: l2Prefix,
          call_id: "call-upstream-canonical",
        },
      ],
    });
    const context = createRuntimeContext({
      command,
      openclawRunId: `openclaw-canonical-read-${stamp}`,
    });
    const readTool = registerOpenVikingTools(context, { baseUrl: OPENVIKING_BASE_URL }).find(
      (item) => item.name === "openviking.read_with_capability",
    );
    expect(readTool).toBeDefined();

    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    fetchMock
      .mockResolvedValueOnce(new Response(l1Display, { status: 200 }))
      .mockResolvedValueOnce(
        new Response(l1CanonicalBytes, {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      )
      .mockResolvedValueOnce(new Response(l2Display, { status: 200 }))
      .mockResolvedValueOnce(
        new Response(l2CanonicalBytes, {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      )
      .mockResolvedValueOnce(new Response(l2IndexDisplay, { status: 200 }))
      .mockResolvedValueOnce(
        new Response(l2IndexBytes, {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      );

    const l1Result = await readTool!.execute("call-read-l1-canonical", {
      material_id: capability.material_id,
      layer: "L1",
    });
    const l1Details = readDetailsRecord(l1Result);
    expect(l1Details.content).toBe(l1Display);
    expect(l1Details.sha256).toBe(l1CanonicalSha);
    expect(l1Details.sha256).not.toBe(l1DisplaySha);

    const l2Result = await readTool!.execute("call-read-l2-canonical", {
      capability_id: capability.capability_id,
      material_id: capability.material_id,
      uri: l2Uri,
      layer: "L2",
    });
    const l2Details = readDetailsRecord(l2Result);
    expect(l2Details.content).toBe(l2Display);
    expect(l2Details.sha256).toBe(l2CanonicalSha);
    expect(l2Details.sha256).not.toBe(l2DisplaySha);
  });
});
