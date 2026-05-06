import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { Type } from "@sinclair/typebox";
import JSZip from "jszip";
import type { AnyAgentTool } from "../../pi-tools.types.js";
import { payloadTextResult } from "../../tools/common.js";
import type { RuntimeContext, SingleWorkerReadCapability } from "./params.js";

const DEFAULT_OPENVIKING_BASE_URL = "http://127.0.0.1:1933";
const OPENVIKING_WRITE_RETRY_LIMIT = 6;
const OPENVIKING_L2_INDEX_WRITE_RETRY_LIMIT = 10;
const OPENVIKING_WRITE_RETRY_BASE_DELAY_MS = 200;
const OPENVIKING_WRITE_RETRY_MAX_DELAY_MS = 2000;
// PM 只能在这个小枚举里给最终评级，避免模型临时造出 HOLD/OBSERVATION 这类不可机读值。
const PM_ALLOWED_RATINGS = ["buy", "hold", "sell", "neutral", "not_rated"] as const;

type OpenVikingToolOptions = {
  baseUrl?: string;
};

type OpenVikingHttpOperation = {
  operation: string;
  method: string;
  endpoint: string;
  status: number;
  ok: boolean;
  error_code?: string;
  error_message?: string;
};

type OpenVikingHttpPayload<T> = {
  status?: string;
  result?: T;
  error?: {
    code?: string;
    message?: string;
  };
};

type OpenVikingStatIntegrity = {
  sizeBytes?: number;
  sha256?: string;
};

class OpenVikingHttpError extends Error {
  readonly statusCode: number;
  readonly errorCode: string;
  readonly responseBody?: string;

  constructor(message: string, statusCode: number, errorCode: string, responseBody?: string) {
    super(message);
    this.name = "OpenVikingHttpError";
    this.statusCode = statusCode;
    this.errorCode = errorCode;
    this.responseBody = responseBody;
  }
}

const PM_DECISION_PARAMETERS = Type.Object(
  {
    rating: Type.Union(PM_ALLOWED_RATINGS.map((value) => Type.Literal(value))),
    final_conclusion: Type.String(),
    execution_conditions: Type.Array(Type.String()),
    risk_conditions: Type.Array(Type.String()),
    source_claim_ids: Type.Optional(Type.Array(Type.String())),
  },
  { additionalProperties: false },
);

const WRITE_MATERIAL_BASE_PARAMETERS = Type.Object(
  {
    uri: Type.Optional(Type.String()),
    target: Type.Optional(Type.String()),
    content: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

const WRITE_MATERIAL_PM_PARAMETERS = Type.Object(
  {
    uri: Type.Optional(Type.String()),
    target: Type.Optional(Type.String()),
    content: Type.Optional(Type.String()),
    pm_decision: Type.Optional(PM_DECISION_PARAMETERS),
  },
  { additionalProperties: false },
);

const READ_WITH_CAPABILITY_PARAMETERS = Type.Object(
  {
    // 模型只看 material_id/layer；capability_id 和 uri 由运行时从 claw-trade command 里解析。
    material_id: Type.String(),
    layer: Type.Optional(Type.Union([Type.Literal("L1"), Type.Literal("L2")])),
  },
  { additionalProperties: false },
);

type MaterialClaimsPayload = {
  schema_version: "control.claims.v1";
  source: "openclaw_openviking_write_material";
  run_id: string;
  call_id: string;
  worker_id: string;
  stage: string;
  material_id: string;
  target_name: string;
  l1_uri: string;
  l1_sha256: string;
  l1_size_bytes: number;
  material_layer: "L1";
  source_kind: "worker_report";
  claims: unknown[];
};

type PmDecisionInput = {
  rating: string;
  final_conclusion: string;
  execution_conditions: string[];
  risk_conditions: string[];
  source_claim_ids: string[];
};

function resolveBaseUrl(options?: OpenVikingToolOptions): string {
  const candidate =
    options?.baseUrl ?? process.env.OPENVIKING_BASE_URL ?? DEFAULT_OPENVIKING_BASE_URL;
  return candidate.trim().replace(/\/+$/, "");
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function readOptionalString(record: Record<string, unknown>, key: string): string | undefined {
  const raw = record[key];
  if (typeof raw !== "string") {
    return undefined;
  }
  const value = raw.trim();
  return value.length > 0 ? value : undefined;
}

function resolveEvidencePath(context: RuntimeContext, fileName: string): string {
  const evidenceRoot = path.resolve(context.evidenceDir);
  const resolved = path.resolve(evidenceRoot, fileName);
  const relative = path.relative(evidenceRoot, resolved);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`evidence path escapes evidenceDir: ${fileName}`);
  }
  return resolved;
}

function containsForbiddenStructuredBlock(content: string): string | null {
  if (/control\.claims\.v1/i.test(content)) {
    return "content contains reserved schema marker control.claims.v1";
  }
  if (/```[\w-]*\s*pm[_-]?decision[\s\S]*?```/i.test(content)) {
    return "content contains fenced PM decision block";
  }
  if (
    /```[\s\S]*?rating[\s\S]*?final_conclusion[\s\S]*?```/i.test(content) ||
    /```[\s\S]*?final_conclusion[\s\S]*?rating[\s\S]*?```/i.test(content)
  ) {
    return "content contains fenced PM decision fields";
  }
  return null;
}

function sha256OfUtf8(content: string): string {
  return crypto.createHash("sha256").update(content, "utf8").digest("hex");
}

function sha256OfBytes(content: Uint8Array): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

function normalizeOpenVikingText(content: string): string {
  return content.replace(/\r\n/g, "\n").replace(/\n+$/g, "");
}

function detectTargetName(uri: string): string {
  const normalized = uri.replace(/\/+$/, "");
  const parts = normalized.split("/");
  const targetName = parts.at(-1) ?? "";
  if (!targetName) {
    throw new Error(`invalid OpenViking file uri: ${uri}`);
  }
  return targetName;
}

function detectParentUri(uri: string): string {
  const normalized = uri.replace(/\/+$/, "");
  const cut = normalized.lastIndexOf("/");
  if (cut <= "viking://".length) {
    throw new Error(`invalid OpenViking file uri: ${uri}`);
  }
  return normalized.slice(0, cut);
}

function encodeQueryUri(uri: string): string {
  return encodeURIComponent(uri);
}

function normalizeReadContent(raw: unknown): string {
  if (typeof raw === "string") {
    return raw;
  }
  return JSON.stringify(raw);
}

function toNonNegativeInteger(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value);
  }
  if (typeof value === "string" && /^[0-9]+$/.test(value.trim())) {
    return Number.parseInt(value.trim(), 10);
  }
  return undefined;
}

function toSha256(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(normalized)) {
    return undefined;
  }
  return normalized;
}

function pickStatTargetNode(raw: unknown, targetName: string): Record<string, unknown> {
  const root = asRecord(raw);
  const candidateNodes: Record<string, unknown>[] = [
    root,
    asRecord(root.result),
    asRecord(root.file),
    asRecord(root.node),
    asRecord(root.entry),
  ];
  for (const candidate of candidateNodes) {
    if (Object.keys(candidate).length === 0) {
      continue;
    }
    const name = readOptionalString(candidate, "name");
    if (name && name !== targetName) {
      continue;
    }
    if (candidate.isDir === true) {
      continue;
    }
    return candidate;
  }
  const entriesRaw = Array.isArray(root.entries)
    ? root.entries
    : Array.isArray(asRecord(root.result).entries)
      ? (asRecord(root.result).entries as unknown[])
      : null;
  if (entriesRaw) {
    for (const entryRaw of entriesRaw) {
      const entry = asRecord(entryRaw);
      if (readOptionalString(entry, "name") === targetName && entry.isDir !== true) {
        return entry;
      }
    }
  }
  throw new Error("openviking.write_material verification stat target node not found");
}

function extractStatIntegrity(params: {
  raw: unknown;
  targetName: string;
}): OpenVikingStatIntegrity {
  const statNode = pickStatTargetNode(params.raw, params.targetName);
  const sizeBytes =
    toNonNegativeInteger(statNode.size_bytes) ??
    toNonNegativeInteger(statNode.size) ??
    toNonNegativeInteger(statNode.file_size);
  if (sizeBytes === undefined) {
    throw new Error("openviking.write_material verification missing stat size after write");
  }
  const checksums = asRecord(statNode.checksums);
  const shaFields = [
    statNode.sha256,
    statNode.content_sha256,
    statNode.file_sha256,
    checksums.sha256,
    checksums.content_sha256,
  ];
  let sha256: string | undefined;
  for (const candidate of shaFields) {
    if (candidate === undefined || candidate === null || candidate === "") {
      continue;
    }
    const normalized = toSha256(candidate);
    if (!normalized) {
      throw new Error("openviking.write_material verification stat sha field is invalid");
    }
    sha256 = normalized;
    break;
  }
  return { sizeBytes, sha256 };
}

function parseL2IndexEntries(indexContent: string): Array<{ uri: string; sha256: string }> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(indexContent);
  } catch {
    throw new Error("openviking.read_with_capability L2 index content is not valid JSON");
  }
  const parsedRecord = asRecord(parsed);
  const entriesRaw = Array.isArray(parsedRecord.entries)
    ? parsedRecord.entries
    : Array.isArray(asRecord(parsedRecord.result).entries)
      ? (asRecord(parsedRecord.result).entries as unknown[])
      : null;
  if (!entriesRaw) {
    throw new Error("openviking.read_with_capability L2 index missing entries");
  }
  const entries: Array<{ uri: string; sha256: string }> = [];
  for (const entryRaw of entriesRaw) {
    const entry = asRecord(entryRaw);
    const uri = readOptionalString(entry, "uri");
    const sha256 =
      toSha256(entry.sha256) ?? toSha256(entry.digest) ?? toSha256(entry.hash) ?? undefined;
    if (!uri || !sha256) {
      continue;
    }
    entries.push({ uri, sha256 });
  }
  if (entries.length === 0) {
    throw new Error("openviking.read_with_capability L2 index has no valid uri/sha256 entries");
  }
  return entries;
}

async function requestOpenViking<T>(params: {
  baseUrl: string;
  endpoint: string;
  method: "GET" | "POST";
  operation: string;
  httpOperations: OpenVikingHttpOperation[];
  body?: BodyInit;
  headers?: Record<string, string>;
}): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${params.baseUrl}${params.endpoint}`, {
      method: params.method,
      body: params.body,
      headers: params.headers,
    });
  } catch (error) {
    params.httpOperations.push({
      operation: params.operation,
      method: params.method,
      endpoint: params.endpoint,
      status: 0,
      ok: false,
      error_code: "TRANSPORT_ERROR",
      error_message: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }

  const rawBody = await response.text();
  let payload: OpenVikingHttpPayload<T> | null = null;
  if (rawBody.trim().length > 0) {
    try {
      payload = JSON.parse(rawBody) as OpenVikingHttpPayload<T>;
    } catch {
      payload = null;
    }
  }

  const payloadStatus = (payload?.status ?? "").toLowerCase();
  const isPayloadError = payloadStatus === "error";
  const rawBodySnippet = rawBody.trim().slice(0, 512);
  const errorCode = payload?.error?.code ?? (response.ok && !isPayloadError ? "" : "HTTP_ERROR");
  const errorMessage =
    payload?.error?.message ??
    (response.ok && !isPayloadError
      ? ""
      : rawBodySnippet
        ? `OpenViking ${params.method} ${params.endpoint} failed: ${rawBodySnippet}`
        : `OpenViking ${params.method} ${params.endpoint} failed`);

  const ok = response.ok && !isPayloadError;
  params.httpOperations.push({
    operation: params.operation,
    method: params.method,
    endpoint: params.endpoint,
    status: response.status,
    ok,
    error_code: errorCode || undefined,
    error_message: errorMessage || undefined,
  });

  if (!ok) {
    throw new OpenVikingHttpError(
      errorMessage || "OpenViking request failed",
      response.status,
      errorCode || "",
      rawBody,
    );
  }

  if (payload && Object.prototype.hasOwnProperty.call(payload, "result")) {
    return payload.result as T;
  }
  if (payload) {
    return payload as unknown as T;
  }
  return rawBody as unknown as T;
}

async function requestOpenVikingBytes(params: {
  baseUrl: string;
  endpoint: string;
  method: "GET";
  operation: string;
  httpOperations: OpenVikingHttpOperation[];
}): Promise<Uint8Array> {
  let response: Response;
  try {
    response = await fetch(`${params.baseUrl}${params.endpoint}`, {
      method: params.method,
    });
  } catch (error) {
    params.httpOperations.push({
      operation: params.operation,
      method: params.method,
      endpoint: params.endpoint,
      status: 0,
      ok: false,
      error_code: "TRANSPORT_ERROR",
      error_message: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }

  const bodyBytes = new Uint8Array(await response.arrayBuffer());
  const bodySnippet = new TextDecoder().decode(bodyBytes.slice(0, 512)).trim();
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";

  let payload: OpenVikingHttpPayload<unknown> | null = null;
  if (contentType.includes("application/json") && bodySnippet.length > 0) {
    try {
      payload = JSON.parse(bodySnippet) as OpenVikingHttpPayload<unknown>;
    } catch {
      payload = null;
    }
  }
  const payloadStatus = (payload?.status ?? "").toLowerCase();
  const isPayloadError = payloadStatus === "error";
  const errorCode = payload?.error?.code ?? (response.ok && !isPayloadError ? "" : "HTTP_ERROR");
  const errorMessage =
    payload?.error?.message ??
    (response.ok && !isPayloadError
      ? ""
      : bodySnippet
        ? `OpenViking ${params.method} ${params.endpoint} failed: ${bodySnippet}`
        : `OpenViking ${params.method} ${params.endpoint} failed`);

  const ok = response.ok && !isPayloadError;
  params.httpOperations.push({
    operation: params.operation,
    method: params.method,
    endpoint: params.endpoint,
    status: response.status,
    ok,
    error_code: errorCode || undefined,
    error_message: errorMessage || undefined,
  });

  if (!ok) {
    throw new OpenVikingHttpError(
      errorMessage || "OpenViking request failed",
      response.status,
      errorCode || "",
      bodySnippet,
    );
  }

  return bodyBytes;
}

function isRetryableOpenVikingTransportError(error: unknown): boolean {
  if (error instanceof OpenVikingHttpError) {
    if (error.statusCode >= 500) {
      return true;
    }
    return false;
  }
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  return (
    message.includes("fetch failed") ||
    message.includes("epipe") ||
    message.includes("write epipe") ||
    message.includes("econnreset") ||
    message.includes("socket hang up") ||
    message.includes("connection reset") ||
    message.includes("network")
  );
}

async function writeAndVerifyWithRetry(params: {
  baseUrl: string;
  uri: string;
  content: string;
  httpOperations: OpenVikingHttpOperation[];
  operationSuffix?: string;
  retryLimit?: number;
}): Promise<VerifiedWriteResult> {
  const retryLimit = Math.max(1, params.retryLimit ?? OPENVIKING_WRITE_RETRY_LIMIT);
  let lastError: unknown;
  for (let attempt = 1; attempt <= retryLimit; attempt += 1) {
    try {
      return await writeAndVerifyOpenVikingFile(params);
    } catch (error) {
      lastError = error;
      if (!isRetryableOpenVikingTransportError(error) || attempt >= retryLimit) {
        throw error;
      }
      params.httpOperations.push({
        operation: `retry.wait${params.operationSuffix ?? ""}`,
        method: "INTERNAL",
        endpoint: params.uri,
        status: 0,
        ok: false,
        error_code: "RETRYABLE_TRANSPORT_ERROR",
        error_message: `attempt=${attempt}`,
      });
      const retryDelayMs = Math.min(
        OPENVIKING_WRITE_RETRY_MAX_DELAY_MS,
        OPENVIKING_WRITE_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1),
      );
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }
  throw lastError;
}

type VerifiedWriteResult = {
  actualSha: string;
  actualSize: number;
  expectedWriteSha: string;
  expectedWriteSize: number;
  statSizeBytes: number;
  statSha256?: string;
};

function resolveL2IndexUri(l2PrefixRaw: string): string {
  const l2Prefix = l2PrefixRaw.trim();
  if (!l2Prefix) {
    throw new Error("openviking.write_material material_target.l2_prefix is required");
  }
  const normalizedPrefix = l2Prefix.endsWith("/") ? l2Prefix : `${l2Prefix}/`;
  return `${normalizedPrefix}index.json`;
}

function buildEmptyL2IndexContent(context: RuntimeContext): string {
  return `${JSON.stringify(
    {
      entries: [],
      empty_reason: "no_evidence",
      run_id: context.command.run_id,
      call_id: context.command.call_id,
      worker_id: context.command.worker_id,
      stage: context.command.stage,
    },
    null,
    2,
  )}\n`;
}

async function writeAndVerifyOpenVikingFile(params: {
  baseUrl: string;
  uri: string;
  content: string;
  httpOperations: OpenVikingHttpOperation[];
  operationSuffix?: string;
}): Promise<VerifiedWriteResult> {
  const suffix = params.operationSuffix ?? "";
  const expectedWriteSha = sha256OfUtf8(params.content);
  const expectedWriteSize = Buffer.byteLength(params.content, "utf8");
  const expectedNormalized = normalizeOpenVikingText(params.content);
  const expectedSha = sha256OfUtf8(expectedNormalized);
  const expectedSize = Buffer.byteLength(expectedNormalized, "utf8");
  const targetName = detectTargetName(params.uri);
  const parentUri = detectParentUri(params.uri);
  const zip = new JSZip();
  zip.file(targetName, params.content);
  const zipBuffer = await zip.generateAsync({ type: "nodebuffer" });
  const zipBytes = Uint8Array.from(zipBuffer);
  const form = new FormData();
  form.append("file", new Blob([zipBytes], { type: "application/zip" }), `${targetName}.zip`);
  const uploadResult = await requestOpenViking<{ temp_file_id?: string }>({
    baseUrl: params.baseUrl,
    endpoint: "/api/v1/resources/temp_upload",
    method: "POST",
    operation: `resources.temp_upload${suffix}`,
    httpOperations: params.httpOperations,
    body: form,
  });
  const tempFileId = uploadResult?.temp_file_id?.trim();
  if (!tempFileId) {
    throw new Error("OpenViking temp upload missing temp_file_id");
  }
  await requestOpenViking<Record<string, unknown>>({
    baseUrl: params.baseUrl,
    endpoint: "/api/v1/pack/import",
    method: "POST",
    operation: `pack.import${suffix}`,
    httpOperations: params.httpOperations,
    body: JSON.stringify({
      temp_file_id: tempFileId,
      parent: parentUri,
      force: true,
      vectorize: false,
    }),
    headers: {
      "Content-Type": "application/json",
    },
  });

  const statAfterWrite = await requestOpenViking<Record<string, unknown>>({
    baseUrl: params.baseUrl,
    endpoint: `/api/v1/fs/stat?uri=${encodeQueryUri(params.uri)}`,
    method: "GET",
    operation: `fs.stat.after_write${suffix}`,
    httpOperations: params.httpOperations,
  });
  const statIntegrity = extractStatIntegrity({
    raw: statAfterWrite,
    targetName,
  });
  const readBack = await requestOpenViking<unknown>({
    baseUrl: params.baseUrl,
    endpoint: `/api/v1/content/read?uri=${encodeQueryUri(params.uri)}`,
    method: "GET",
    operation: `content.read.after_write${suffix}`,
    httpOperations: params.httpOperations,
  });
  const downloadBackBytes = await requestOpenVikingBytes({
    baseUrl: params.baseUrl,
    endpoint: `/api/v1/content/download?uri=${encodeQueryUri(params.uri)}`,
    method: "GET",
    operation: `content.download.after_write${suffix}`,
    httpOperations: params.httpOperations,
  });
  const contentReadBackRaw = normalizeReadContent(readBack);
  const contentReadBackNormalized = normalizeOpenVikingText(contentReadBackRaw);
  const actualSha = sha256OfBytes(downloadBackBytes);
  const actualSize = downloadBackBytes.byteLength;
  // 正式指纹只认 content/download 原始字节；content/read 仅用于文本展示/可读性校验。
  if (actualSha !== expectedWriteSha && actualSha !== expectedSha) {
    throw new Error("openviking.write_material verification sha mismatch after download");
  }
  if (actualSize !== expectedWriteSize && actualSize !== expectedSize) {
    throw new Error("openviking.write_material verification size mismatch after download");
  }
  if (sha256OfUtf8(contentReadBackNormalized) !== expectedSha) {
    throw new Error("openviking.write_material verification sha mismatch after write");
  }
  if (Buffer.byteLength(contentReadBackNormalized, "utf8") !== expectedSize) {
    throw new Error("openviking.write_material verification size mismatch after write");
  }
  if (
    statIntegrity.sizeBytes !== expectedWriteSize &&
    statIntegrity.sizeBytes !== expectedSize &&
    statIntegrity.sizeBytes !== actualSize
  ) {
    throw new Error("openviking.write_material verification stat size mismatch after readback");
  }
  if (
    statIntegrity.sha256 &&
    statIntegrity.sha256 !== expectedWriteSha &&
    statIntegrity.sha256 !== expectedSha &&
    statIntegrity.sha256 !== actualSha
  ) {
    throw new Error("openviking.write_material verification stat sha mismatch");
  }

  return {
    // receipt 的 sha/size 绑定 content/download 原始字节；控制侧审批和运行时读取使用同一个事实源。
    actualSha,
    actualSize,
    expectedWriteSha,
    expectedWriteSize,
    statSizeBytes: statIntegrity.sizeBytes,
    statSha256: statIntegrity.sha256,
  };
}

async function writeReceipt(params: {
  context: RuntimeContext;
  uri: string;
  sha256: string;
  sizeBytes: number;
  expectedWriteSha256: string;
  expectedWriteSizeBytes: number;
  statSizeBytes: number;
  statSha256?: string;
  httpOperations: OpenVikingHttpOperation[];
}): Promise<string> {
  await fs.mkdir(params.context.evidenceDir, { recursive: true });
  const receiptPath = resolveEvidencePath(params.context, "openviking-write-receipt.json");
  await fs.writeFile(
    receiptPath,
    `${JSON.stringify(
      {
        source: "openviking_adapter_verified_receipt",
        receipt_id: buildAdapterReceiptId(params),
        receipt_label: "verified_openviking_write_receipt",
        receipt_origin: "adapter_verified_non_native",
        is_openviking_native_receipt: false,
        uri: params.uri,
        sha256: params.sha256,
        size_bytes: params.sizeBytes,
        run_id: params.context.command.run_id,
        stage: params.context.command.stage,
        worker_id: params.context.command.worker_id,
        call_id: params.context.command.call_id,
        target_name: params.context.command.material_target.target_name,
        verification: {
          verified: true,
          // 兼容现有 guard 字段名；真实校验对象仍是下载回来的标准内容字节。
          method: "openviking_write_then_stat_then_readback_sha_size_identity_check",
          expected_write_sha256: params.expectedWriteSha256,
          expected_write_size_bytes: params.expectedWriteSizeBytes,
          expected_sha256: params.sha256,
          expected_size_bytes: params.sizeBytes,
          stat_size_bytes: params.statSizeBytes,
          stat_sha256: params.statSha256 ?? null,
          readback_sha256: params.sha256,
          readback_size_bytes: params.sizeBytes,
        },
        written_at: new Date().toISOString(),
        http_operations: params.httpOperations,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  return receiptPath;
}

function buildAdapterReceiptId(params: {
  context: RuntimeContext;
  uri: string;
  sha256: string;
}): string {
  const fingerprint = crypto
    .createHash("sha256")
    .update(
      [
        params.context.command.run_id,
        params.context.command.call_id,
        params.context.command.worker_id,
        params.uri,
        params.sha256,
      ].join("|"),
      "utf8",
    )
    .digest("hex")
    .slice(0, 12);
  return `adapter-${params.context.command.run_id}-${params.context.command.call_id}-${fingerprint}`;
}

function buildMaterialId(params: {
  runId: string;
  callId: string;
  workerId: string;
  stage: string;
  targetName: string;
  uri: string;
  sha256: string;
}): string {
  const digest = crypto
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
    .slice(0, 24);
  return `mat-${digest}`;
}

async function writeMaterialClaimsFile(params: {
  context: RuntimeContext;
  uri: string;
  sha256: string;
  sizeBytes: number;
}): Promise<{ claimsPath: string; payload: MaterialClaimsPayload }> {
  await fs.mkdir(params.context.evidenceDir, { recursive: true });
  const materialId = buildMaterialId({
    runId: params.context.command.run_id,
    callId: params.context.command.call_id,
    workerId: params.context.command.worker_id,
    stage: params.context.command.stage,
    targetName: params.context.command.material_target.target_name,
    uri: params.uri,
    sha256: params.sha256,
  });
  const payload: MaterialClaimsPayload = {
    schema_version: "control.claims.v1",
    source: "openclaw_openviking_write_material",
    run_id: params.context.command.run_id,
    call_id: params.context.command.call_id,
    worker_id: params.context.command.worker_id,
    stage: params.context.command.stage,
    material_id: materialId,
    target_name: params.context.command.material_target.target_name,
    l1_uri: params.uri,
    l1_sha256: params.sha256,
    l1_size_bytes: params.sizeBytes,
    material_layer: "L1",
    source_kind: "worker_report",
    claims: [],
  };
  const claimsPath = resolveEvidencePath(params.context, "material-claims.json");
  await fs.writeFile(claimsPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return { claimsPath, payload };
}

function parsePmDecision(args: Record<string, unknown>): PmDecisionInput | undefined {
  // PM 决策走结构化参数，不再让模型把 rating/final_conclusion 拼在正文里等 Python 猜。
  if (!Object.prototype.hasOwnProperty.call(args, "pm_decision")) {
    return undefined;
  }
  const raw = args.pm_decision;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("openviking.write_material pm_decision must be an object");
  }
  const pm = raw as Record<string, unknown>;
  const sourceClaimIdsRaw = pm.source_claim_ids;
  if (sourceClaimIdsRaw !== undefined && !Array.isArray(sourceClaimIdsRaw)) {
    throw new Error("openviking.write_material pm_decision.source_claim_ids must be an array");
  }
  const sourceClaimIds = Array.isArray(sourceClaimIdsRaw)
    ? sourceClaimIdsRaw.map((item) => {
        if (typeof item !== "string") {
          throw new Error(
            "openviking.write_material pm_decision.source_claim_ids item must be string",
          );
        }
        return item;
      })
    : [];
  const readPmField = (field: string): string => {
    const value = pm[field];
    if (typeof value !== "string" || value.trim().length === 0) {
      throw new Error(`openviking.write_material pm_decision.${field} must be non-empty`);
    }
    return value;
  };
  const readPmStringArrayField = (field: string): string[] => {
    const value = pm[field];
    if (!Array.isArray(value)) {
      throw new Error(`openviking.write_material pm_decision.${field} must be an array`);
    }
    return value.map((item) => {
      if (typeof item !== "string" || item.trim().length === 0) {
        throw new Error(
          `openviking.write_material pm_decision.${field} item must be non-empty string`,
        );
      }
      return item;
    });
  };
  return {
    rating: (() => {
      const rating = readPmField("rating");
      if (!PM_ALLOWED_RATINGS.includes(rating as (typeof PM_ALLOWED_RATINGS)[number])) {
        throw new Error(
          `openviking.write_material pm_decision.rating must be one of: ${PM_ALLOWED_RATINGS.join(", ")}`,
        );
      }
      return rating;
    })(),
    final_conclusion: readPmField("final_conclusion"),
    execution_conditions: readPmStringArrayField("execution_conditions"),
    risk_conditions: readPmStringArrayField("risk_conditions"),
    source_claim_ids: sourceClaimIds,
  };
}

function isPmDecisionScope(context: RuntimeContext): boolean {
  return (
    context.command.worker_id === "portfolio_manager" &&
    context.command.stage === "portfolio_decision"
  );
}

function assertPmDecisionScope(context: RuntimeContext): void {
  // 只有 portfolio_manager 的最后阶段能写 PM 决策，其他 worker 即使传了参数也会失败。
  if (!isPmDecisionScope(context)) {
    throw new Error(
      "openviking.write_material pm_decision is only allowed for portfolio_manager@portfolio_decision",
    );
  }
}

function writeMaterialParametersForContext(context: RuntimeContext) {
  return isPmDecisionScope(context) ? WRITE_MATERIAL_PM_PARAMETERS : WRITE_MATERIAL_BASE_PARAMETERS;
}

async function writePmDecisionFile(params: {
  context: RuntimeContext;
  pmDecision: PmDecisionInput;
  materialClaims: MaterialClaimsPayload;
}): Promise<string> {
  await fs.mkdir(params.context.evidenceDir, { recursive: true });
  const pmDecisionPath = resolveEvidencePath(params.context, "pm-decision.json");
  await fs.writeFile(
    pmDecisionPath,
    `${JSON.stringify(
      {
        schema_version: "control.pm_decision.v1",
        run_id: params.context.command.run_id,
        call_id: params.context.command.call_id,
        worker_id: "portfolio_manager",
        stage: "portfolio_decision",
        material_id: params.materialClaims.material_id,
        rating: params.pmDecision.rating,
        final_conclusion: params.pmDecision.final_conclusion,
        execution_conditions: params.pmDecision.execution_conditions,
        risk_conditions: params.pmDecision.risk_conditions,
        source_claim_ids: params.pmDecision.source_claim_ids,
        source_l1_sha256: params.materialClaims.l1_sha256,
        l1_uri: params.materialClaims.l1_uri,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  return pmDecisionPath;
}

function ensureCapabilityAccess(params: { capability: SingleWorkerReadCapability; uri: string }): {
  isL1: boolean;
} {
  const uri = params.uri.trim();
  const isL1 = uri === params.capability.allowed_l1_uri;
  if (isL1) {
    return { isL1: true };
  }
  const prefix = params.capability.allowed_l2_prefix?.trim() ?? "";
  if (prefix && uri.startsWith(prefix)) {
    return { isL1: false };
  }
  throw new Error("openviking.read_with_capability uri does not match capability scope");
}

function resolveReadRequest(params: { context: RuntimeContext; args: Record<string, unknown> }): {
  capability: SingleWorkerReadCapability;
  uri: string;
  layer: "L1" | "L2";
} {
  // 读路径由 command 里的 approved capability 决定，worker 不需要也不应该手写 URI。
  const materialId = readOptionalString(params.args, "material_id");
  if (!materialId) {
    throw new Error("openviking.read_with_capability requires material_id");
  }
  const capabilityId = readOptionalString(params.args, "capability_id");
  const uriArg = readOptionalString(params.args, "uri");
  const layerArg = readOptionalString(params.args, "layer");
  if (layerArg && layerArg !== "L1" && layerArg !== "L2") {
    throw new Error("openviking.read_with_capability layer must be L1 or L2");
  }
  const candidates = params.context.command.openviking_read_capabilities.filter(
    (item) => item.material_id === materialId,
  );
  if (candidates.length === 0) {
    throw new Error("openviking.read_with_capability capability not found in command");
  }

  const resolveSingleCandidate = (): SingleWorkerReadCapability => {
    if (candidates.length !== 1) {
      throw new Error(
        "openviking.read_with_capability requires capability_id when material_id is ambiguous",
      );
    }
    return candidates[0];
  };

  let capability: SingleWorkerReadCapability;
  if (capabilityId) {
    const found = candidates.find((item) => item.capability_id === capabilityId);
    if (!found) {
      throw new Error("openviking.read_with_capability capability not found in command");
    }
    capability = found;
  } else {
    if (uriArg) {
      const inScope = candidates.filter((item) => {
        try {
          ensureCapabilityAccess({ capability: item, uri: uriArg });
          return true;
        } catch {
          return false;
        }
      });
      if (inScope.length === 0) {
        throw new Error("openviking.read_with_capability uri does not match capability scope");
      }
      if (inScope.length > 1) {
        throw new Error(
          "openviking.read_with_capability requires capability_id when material_id+uri is ambiguous",
        );
      }
      capability = inScope[0];
    } else {
      capability = resolveSingleCandidate();
    }
  }

  let uri = uriArg;
  if (!uri) {
    if (layerArg === "L2") {
      throw new Error("openviking.read_with_capability L2 reads require uri");
    }
    uri = capability.allowed_l1_uri;
  }
  const access = ensureCapabilityAccess({ capability, uri });
  if (layerArg === "L1" && !access.isL1) {
    throw new Error("openviking.read_with_capability layer=L1 requires L1 uri");
  }
  if (layerArg === "L2" && access.isL1) {
    throw new Error("openviking.read_with_capability layer=L2 requires L2 uri");
  }
  return {
    capability,
    uri,
    layer: access.isL1 ? "L1" : "L2",
  };
}

// OpenViking 工具只能使用本次 command 的写入目标和 manifest capability，防止裸 URI 读取或越权写入。
export function registerOpenVikingTools(
  context: RuntimeContext,
  options?: OpenVikingToolOptions,
): AnyAgentTool[] {
  const baseUrl = resolveBaseUrl(options);

  const writeTool: AnyAgentTool = {
    name: "openviking.write_material",
    label: "openviking.write_material",
    description: "Write worker material to OpenViking target URI from current command.",
    parameters: writeMaterialParametersForContext(context),
    execute: async (toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const pmDecision = parsePmDecision(args);
      if (pmDecision) {
        assertPmDecisionScope(context);
      }
      const requestedTarget = readOptionalString(args, "uri") ?? readOptionalString(args, "target");
      const targetUri = context.command.material_target.l1_uri.trim();
      if (!requestedTarget) {
        throw new Error("openviking.write_material requires uri or target");
      }
      if (requestedTarget !== targetUri) {
        throw new Error(
          "openviking.write_material target must equal command.material_target.l1_uri",
        );
      }
      const contentRaw = args.content;
      if (typeof contentRaw !== "string" || contentRaw.trim().length === 0) {
        throw new Error("openviking.write_material content must be non-empty");
      }
      const forbiddenReason = containsForbiddenStructuredBlock(contentRaw);
      if (forbiddenReason) {
        throw new Error(
          `openviking.write_material content must be worker analysis only: ${forbiddenReason}`,
        );
      }
      const content = contentRaw;
      const httpOperations: OpenVikingHttpOperation[] = [];
      // 写后立刻 stat/read/download 校验，receipt 只是这组真实调用的审计记录，不是假成功凭证。
      const l1WriteResult = await writeAndVerifyWithRetry({
        baseUrl,
        uri: targetUri,
        content,
        httpOperations,
      });
      const l2IndexUri = resolveL2IndexUri(context.command.material_target.l2_prefix);
      const emptyL2IndexContent = buildEmptyL2IndexContent(context);
      await writeAndVerifyWithRetry({
        baseUrl,
        uri: l2IndexUri,
        content: emptyL2IndexContent,
        httpOperations,
        operationSuffix: ".l2_index",
        retryLimit: OPENVIKING_L2_INDEX_WRITE_RETRY_LIMIT,
      });

      const receiptPath = await writeReceipt({
        context,
        uri: targetUri,
        sha256: l1WriteResult.actualSha,
        sizeBytes: l1WriteResult.actualSize,
        expectedWriteSha256: l1WriteResult.expectedWriteSha,
        expectedWriteSizeBytes: l1WriteResult.expectedWriteSize,
        statSizeBytes: l1WriteResult.statSizeBytes,
        statSha256: l1WriteResult.statSha256,
        httpOperations,
      });
      const { claimsPath, payload: materialClaims } = await writeMaterialClaimsFile({
        context,
        uri: targetUri,
        sha256: l1WriteResult.actualSha,
        sizeBytes: l1WriteResult.actualSize,
      });
      const pmDecisionPath = pmDecision
        ? await writePmDecisionFile({
            context,
            pmDecision,
            materialClaims,
          })
        : undefined;
      return payloadTextResult({
        receipt_path: receiptPath,
        claims_path: claimsPath,
        pm_decision_path: pmDecisionPath,
        uri: targetUri,
        sha256: l1WriteResult.actualSha,
      });
    },
  };

  const readTool: AnyAgentTool = {
    name: "openviking.read_with_capability",
    label: "openviking.read_with_capability",
    description:
      "Read approved material from OpenViking using manifest-scoped capability. Preferred args: material_id + layer.",
    parameters: READ_WITH_CAPABILITY_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const { capability, uri, layer } = resolveReadRequest({ context, args });
      const httpOperations: OpenVikingHttpOperation[] = [];
      // 读材料时同时走文本 read 和字节 download：前者给 worker 看，后者用于 guard 指纹校验。
      const readResult = await requestOpenViking<unknown>({
        baseUrl,
        endpoint: `/api/v1/content/read?uri=${encodeQueryUri(uri)}`,
        method: "GET",
        operation: "content.read.with_capability",
        httpOperations,
      });
      const downloadBytes = await requestOpenVikingBytes({
        baseUrl,
        endpoint: `/api/v1/content/download?uri=${encodeQueryUri(uri)}`,
        method: "GET",
        operation: "content.download.with_capability",
        httpOperations,
      });
      const readContentRaw = normalizeReadContent(readResult);
      let content = readContentRaw;
      // capability 指纹校验统一按 content/download 原始字节；content/read 仅返回给 worker 展示文本。
      let sha256 = sha256OfBytes(downloadBytes);
      if (layer === "L1" && sha256 !== capability.allowed_l1_sha256) {
        throw new Error("openviking.read_with_capability L1 sha256 mismatch");
      }
      if (layer === "L2") {
        const expectedIndexSha = capability.allowed_l2_index_sha256?.trim() ?? "";
        if (!expectedIndexSha) {
          throw new Error(
            "openviking.read_with_capability L2 read requires capability.allowed_l2_index_sha256",
          );
        }
        const materialRef = context.command.upstream_materials.find(
          (item) =>
            item.material_id === capability.material_id &&
            item.capability_id === capability.capability_id,
        );
        const l2IndexUri = materialRef?.l2_index_uri?.trim() ?? "";
        if (!l2IndexUri) {
          throw new Error("openviking.read_with_capability L2 read missing upstream l2_index_uri");
        }
        const l2Prefix = capability.allowed_l2_prefix?.trim() ?? "";
        if (l2Prefix && !l2IndexUri.startsWith(l2Prefix)) {
          throw new Error(
            "openviking.read_with_capability L2 index uri is outside capability.allowed_l2_prefix",
          );
        }
        const l2IndexRaw = await requestOpenViking<unknown>({
          baseUrl,
          endpoint: `/api/v1/content/read?uri=${encodeQueryUri(l2IndexUri)}`,
          method: "GET",
          operation: "content.read.l2_index.with_capability",
          httpOperations,
        });
        const l2IndexDownloadBytes = await requestOpenVikingBytes({
          baseUrl,
          endpoint: `/api/v1/content/download?uri=${encodeQueryUri(l2IndexUri)}`,
          method: "GET",
          operation: "content.download.l2_index.with_capability",
          httpOperations,
        });
        const l2IndexContent = normalizeOpenVikingText(normalizeReadContent(l2IndexRaw));
        const l2IndexSha = sha256OfBytes(l2IndexDownloadBytes);
        if (l2IndexSha !== expectedIndexSha) {
          throw new Error("openviking.read_with_capability L2 index sha256 mismatch");
        }
        const l2Entries = parseL2IndexEntries(l2IndexContent);
        const expectedEntry = l2Entries.find((entry) => entry.uri === uri);
        if (!expectedEntry) {
          throw new Error("openviking.read_with_capability L2 uri not found in index entries");
        }
        if (expectedEntry.sha256 !== sha256) {
          throw new Error("openviking.read_with_capability L2 content sha256 mismatch");
        }
      }
      return payloadTextResult({
        content,
        uri,
        sha256,
        material_id: capability.material_id,
        capability_id: capability.capability_id,
        layer,
      });
    },
  };

  return [writeTool, readTool];
}
