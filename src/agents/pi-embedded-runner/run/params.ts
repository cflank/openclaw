import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { ImageContent } from "@mariozechner/pi-ai";
import YAML from "yaml";
import type { SourceReplyDeliveryMode } from "../../../auto-reply/get-reply-options.types.js";
import type { ReplyPayload } from "../../../auto-reply/reply-payload.js";
import type { ReplyOperation } from "../../../auto-reply/reply/reply-run-registry.js";
import type { ReasoningLevel, ThinkLevel, VerboseLevel } from "../../../auto-reply/thinking.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import type { PromptImageOrderEntry } from "../../../media/prompt-image-order.js";
import type { CommandQueueEnqueueFn } from "../../../process/command-queue.types.js";
import type { InputProvenance } from "../../../sessions/input-provenance.js";
import type { ExecElevatedDefaults, ExecToolDefaults } from "../../bash-tools.exec-types.js";
import type { AgentStreamParams, ClientToolDefinition } from "../../command/shared-types.js";
import type { AgentInternalEvent } from "../../internal-events.js";
import type { BlockReplyPayload } from "../../pi-embedded-payloads.js";
import type {
  BlockReplyChunking,
  ToolResultFormat,
} from "../../pi-embedded-subscribe.shared-types.js";
import type { SkillSnapshot } from "../../skills.js";
import type { SilentReplyPromptMode } from "../../system-prompt.types.js";
import type { PromptMode } from "../../system-prompt.types.js";
import type { AuthProfileFailurePolicy } from "./auth-profile-failure-policy.types.js";
export type { ClientToolDefinition } from "../../command/shared-types.js";

export type EmbeddedRunTrigger = "cron" | "heartbeat" | "manual" | "memory" | "overflow" | "user";

export type SingleWorkerMaterialReadRef = {
  material_id: string;
  capability_id: string;
  worker_id: string;
  stage: string;
  l1_uri: string;
  l1_sha256: string;
  l2_index_uri?: string | null;
  l2_allowed_prefix?: string | null;
  call_id: string;
};

export type SingleWorkerReadCapability = {
  capability_id: string;
  material_id: string;
  allowed_l1_uri: string;
  allowed_l1_sha256: string;
  allowed_l2_prefix?: string | null;
  allowed_l2_index_sha256?: string | null;
  manifest_entry_sha256: string;
};

export type SingleWorkerMaterialTarget = {
  run_id: string;
  call_id: string;
  worker_id: string;
  stage: string;
  target_name: string;
  l1_uri: string;
  l2_prefix: string;
};

export type SingleWorkerReadPolicy = {
  default_layer: string;
  allow_l2_when: string[];
  forbid_compact_as_writing_source: boolean;
};

export type SingleWorkerCommand = {
  // claw-trade 发来的单 worker 命令；OpenClaw 只按它运行一轮，不拥有整条交易工作流。
  agent: string;
  run_id: string;
  call_id: string;
  worker_id: string;
  stage: string;
  profile: string;
  runtime_vars: Record<string, string>;
  allowed_tools: string[];
  evidence_dir: string;
  upstream_materials: SingleWorkerMaterialReadRef[];
  openviking_read_capabilities: SingleWorkerReadCapability[];
  material_target: SingleWorkerMaterialTarget;
  read_policy: SingleWorkerReadPolicy;
  stop_after_first_response: boolean;
};

const OPENVIKING_MATERIAL_BRIEF_HEADER = "[OpenVikingReadableMaterials]";
const OPENVIKING_WRITE_TARGET_HEADER = "[OpenVikingWriteTarget]";
const RUNTIME_TARGET_HEADER = "[RuntimeTarget]";
const RUNTIME_PLACEHOLDER_PATTERN = /\{([a-zA-Z_][a-zA-Z0-9_]*)\}/g;

export type RuntimeMarkers = {
  run_id: string;
  call_id: string;
  worker_id: string;
  stage: string;
  profile: string;
  openclaw_run_id: string;
};

export type RuntimeContext = {
  command: SingleWorkerCommand;
  evidenceDir: string;
  markers: RuntimeMarkers;
};

export type WorkspaceEvidence = {
  source: "openclaw_workspace_loader";
  run_id: string;
  call_id: string;
  worker_id: string;
  stage: string;
  profile: string;
  openclaw_run_id: string;
  workspace_root: string;
  identity: { path: string; sha256: string };
  skills_manifest: { path: string; sha256: string };
  stage_policy: { path: string; sha256: string; selected_stage: string };
  text_assets: Array<{ path: string; sha256: string }>;
  loaded_at: string;
};

export type RunEmbeddedPiAgentParams = {
  sessionId: string;
  sessionKey?: string;
  /** Session-like key for sandbox and tool-policy resolution. Defaults to sessionKey. */
  sandboxSessionKey?: string;
  agentId?: string;
  messageChannel?: string;
  messageProvider?: string;
  agentAccountId?: string;
  /** What initiated this agent run: "user", "heartbeat", "cron", "memory", "overflow", or "manual". */
  trigger?: EmbeddedRunTrigger;
  /** Stable cron job identifier populated for cron-triggered runs. */
  jobId?: string;
  /** Relative workspace path that memory-triggered writes are allowed to append to. */
  memoryFlushWritePath?: string;
  /** Delivery target for topic/thread routing. */
  messageTo?: string;
  /** Thread/topic identifier for routing replies to the originating thread. */
  messageThreadId?: string | number;
  /** Group id for channel-level tool policy resolution. */
  groupId?: string | null;
  /** Group channel label (e.g. #general) for channel-level tool policy resolution. */
  groupChannel?: string | null;
  /** Group space label (e.g. guild/team id) for channel-level tool policy resolution. */
  groupSpace?: string | null;
  /** Trusted provider role ids for the requester in this group turn. */
  memberRoleIds?: string[];
  /** Parent session key for subagent policy inheritance. */
  spawnedBy?: string | null;
  /** Whether workspaceDir points at the canonical agent workspace for bootstrap purposes. */
  isCanonicalWorkspace?: boolean;
  senderId?: string | null;
  senderName?: string | null;
  senderUsername?: string | null;
  senderE164?: string | null;
  /** Whether the sender is an owner (required for owner-only tools). */
  senderIsOwner?: boolean;
  /**
   * Additional owner-only tools authorized by a server-side runtime grant.
   * This must stay narrow; it does not make the sender an owner.
   */
  ownerOnlyToolAllowlist?: string[];
  /** Current channel ID for auto-threading (Slack). */
  currentChannelId?: string;
  /** Current thread timestamp for auto-threading (Slack). */
  currentThreadTs?: string;
  /** Current inbound message id for action fallbacks (e.g. Telegram react). */
  currentMessageId?: string | number;
  /** Reply-to mode for Slack auto-threading. */
  replyToMode?: "off" | "first" | "all" | "batched";
  /** Mutable ref to track if a reply was sent (for "first" mode). */
  hasRepliedRef?: { value: boolean };
  /** Require explicit message tool targets (no implicit last-route sends). */
  requireExplicitMessageTarget?: boolean;
  /** If true, omit the message tool from the tool list. */
  disableMessageTool?: boolean;
  /** Internal one-shot model probe mode: no tools, no workspace/chat prompt policy. */
  modelRun?: boolean;
  /** Explicit system prompt mode override for trusted callers. */
  promptMode?: PromptMode;
  /** Keep the message tool available even when a narrow profile would omit it. */
  forceMessageTool?: boolean;
  /** Allow runtime plugins for this run to late-bind the gateway subagent. */
  allowGatewaySubagentBinding?: boolean;
  sessionFile: string;
  workspaceDir: string;
  agentDir?: string;
  config?: OpenClawConfig;
  skillsSnapshot?: SkillSnapshot;
  prompt: string;
  /** User-visible prompt body to submit and persist; runtime context travels separately. */
  transcriptPrompt?: string;
  images?: ImageContent[];
  imageOrder?: PromptImageOrderEntry[];
  /** Optional client-provided tools (OpenResponses hosted tools). */
  clientTools?: ClientToolDefinition[];
  /** Disable built-in tools for this run (LLM-only mode). */
  disableTools?: boolean;
  provider?: string;
  model?: string;
  /** Effective model fallback chain for this session attempt. Undefined uses config defaults. */
  modelFallbacksOverride?: string[];
  /** Session-pinned embedded harness id. Prevents runtime hot-switching. */
  agentHarnessId?: string;
  authProfileId?: string;
  authProfileIdSource?: "auto" | "user";
  thinkLevel?: ThinkLevel;
  fastMode?: boolean;
  verboseLevel?: VerboseLevel;
  reasoningLevel?: ReasoningLevel;
  toolResultFormat?: ToolResultFormat;
  /** If true, suppress tool error warning payloads for this run (including mutating tools). */
  suppressToolErrorWarnings?: boolean;
  /** Bootstrap context mode for workspace file injection. */
  bootstrapContextMode?: "full" | "lightweight";
  /** Run kind hint for context mode behavior. */
  bootstrapContextRunKind?: "default" | "heartbeat" | "cron";
  /** Optional tool allow-list; when set, only these tools are sent to the model. */
  toolsAllow?: string[];
  /** Seen bootstrap truncation warning signatures for this session (once mode dedupe). */
  bootstrapPromptWarningSignaturesSeen?: string[];
  /** Last shown bootstrap truncation warning signature for this session. */
  bootstrapPromptWarningSignature?: string;
  execOverrides?: Pick<
    ExecToolDefaults,
    "host" | "security" | "ask" | "node" | "notifyOnExit" | "notifyOnExitEmptySuccess"
  >;
  bashElevated?: ExecElevatedDefaults;
  timeoutMs: number;
  runId: string;
  abortSignal?: AbortSignal;
  onExecutionStarted?: () => void;
  replyOperation?: ReplyOperation;
  shouldEmitToolResult?: () => boolean;
  shouldEmitToolOutput?: () => boolean;
  onPartialReply?: (payload: { text?: string; mediaUrls?: string[] }) => void | Promise<void>;
  onAssistantMessageStart?: () => void | Promise<void>;
  onBlockReply?: (payload: BlockReplyPayload) => void | Promise<void>;
  onBlockReplyFlush?: () => void | Promise<void>;
  blockReplyBreak?: "text_end" | "message_end";
  blockReplyChunking?: BlockReplyChunking;
  onReasoningStream?: (payload: { text?: string; mediaUrls?: string[] }) => void | Promise<void>;
  onReasoningEnd?: () => void | Promise<void>;
  onToolResult?: (payload: ReplyPayload) => void | Promise<void>;
  onAgentEvent?: (evt: {
    stream: string;
    data: Record<string, unknown>;
    sessionKey?: string;
  }) => void;
  lane?: string;
  enqueue?: CommandQueueEnqueueFn;
  extraSystemPrompt?: string;
  sourceReplyDeliveryMode?: SourceReplyDeliveryMode;
  silentReplyPromptMode?: SilentReplyPromptMode;
  internalEvents?: AgentInternalEvent[];
  inputProvenance?: InputProvenance;
  streamParams?: AgentStreamParams;
  ownerNumbers?: string[];
  enforceFinalTag?: boolean;
  silentExpected?: boolean;
  /**
   * Treat a clean empty assistant stop as an intentional silent reply.
   * Only set when the caller's prompt policy already allows an exact NO_REPLY
   * final answer for silence.
   */
  allowEmptyAssistantReplyAsSilent?: boolean;
  authProfileFailurePolicy?: AuthProfileFailurePolicy;
  /**
   * Allow a single run attempt even when all auth profiles are in cooldown,
   * but only for inferred transient cooldowns like `rate_limit` or `overloaded`.
   *
   * This is used by model fallback when trying sibling models on providers
   * where transient service pressure is often model-scoped.
   */
  allowTransientCooldownProbe?: boolean;
  /**
   * Dispose bundled MCP runtimes when the overall run ends instead of preserving
   * the session-scoped cache. Intended for one-shot local CLI runs that must
   * exit promptly after emitting the final JSON result.
   */
  cleanupBundleMcpOnRunEnd?: boolean;
  /**
   * control migration seam: machine-readable single-worker command from claw-trade.
   * 只做单 worker 运行上下文透传，不承载 claw-trade DAG 业务流程。
   */
  singleWorkerCommand?: SingleWorkerCommand;
};

function assertNonEmptyString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`singleWorkerCommand.${fieldName} must be a non-empty string`);
  }
  return value;
}

function sha256ForBuffer(buffer: Buffer): string {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

async function sha256ForFile(filePath: string): Promise<string> {
  return sha256ForBuffer(await fs.readFile(filePath));
}

async function maybeSha256ForFile(filePath: string): Promise<string> {
  const stat = await fs.stat(filePath);
  if (!stat.isFile()) {
    throw new Error(`workspace evidence expected file: ${filePath}`);
  }
  return await sha256ForFile(filePath);
}

async function collectTextAssets(
  workspaceRoot: string,
): Promise<Array<{ path: string; sha256: string }>> {
  const textAssets: Array<{ path: string; sha256: string }> = [];
  const queue = [workspaceRoot];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      continue;
    }
    const entries = await fs.readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        queue.push(fullPath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      const normalized = fullPath.replaceAll("\\", "/");
      const isTextAsset =
        normalized.endsWith(".md") || normalized.endsWith(".yaml") || normalized.endsWith(".yml");
      if (!isTextAsset) {
        continue;
      }
      if (
        normalized.endsWith("/IDENTITY.md") ||
        normalized.endsWith("/skills/manifest.yaml") ||
        normalized.endsWith("/STAGES.yaml")
      ) {
        continue;
      }
      textAssets.push({ path: fullPath, sha256: await sha256ForFile(fullPath) });
    }
  }
  textAssets.sort((a, b) => a.path.localeCompare(b.path));
  return textAssets;
}

export function createRuntimeContext(params: {
  command: SingleWorkerCommand;
  openclawRunId: string;
}): RuntimeContext {
  // 这些 marker 会写进所有证据文件，方便控制侧确认每份证据属于同一个 run/call/worker。
  const command = params.command;
  const markers: RuntimeMarkers = {
    run_id: assertNonEmptyString(command.run_id, "run_id"),
    call_id: assertNonEmptyString(command.call_id, "call_id"),
    worker_id: assertNonEmptyString(command.worker_id, "worker_id"),
    stage: assertNonEmptyString(command.stage, "stage"),
    profile: assertNonEmptyString(command.profile, "profile"),
    openclaw_run_id: assertNonEmptyString(params.openclawRunId, "openclawRunId"),
  };
  return {
    command,
    evidenceDir: assertNonEmptyString(command.evidence_dir, "evidence_dir"),
    markers,
  };
}

export async function ensureEvidenceDir(command: SingleWorkerCommand): Promise<void> {
  const evidenceDir = assertNonEmptyString(command.evidence_dir, "evidence_dir");
  await fs.mkdir(evidenceDir, { recursive: true });
}

function asObjectRecord(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`single-worker prompt config must be an object: ${label}`);
  }
  return value as Record<string, unknown>;
}

function asRequiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`single-worker prompt config missing required string: ${label}`);
  }
  return value.trim();
}

function resolvePromptTemplatePath(params: {
  workspaceRoot: string;
  stagePolicyPath: string;
  promptRelativePath: string;
}): string {
  const workspaceRoot = path.resolve(params.workspaceRoot);
  const promptTemplatePath = path.resolve(workspaceRoot, params.promptRelativePath);
  const relative = path.relative(workspaceRoot, promptTemplatePath);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(
      `single-worker prompt path escapes workspace: ${params.promptRelativePath} (stage policy: ${params.stagePolicyPath})`,
    );
  }
  return promptTemplatePath;
}

export function renderSingleWorkerPromptTemplate(
  template: string,
  runtimeVars: Record<string, string>,
): string {
  const missingVars = new Set<string>();
  const rendered = template.replaceAll(RUNTIME_PLACEHOLDER_PATTERN, (full, varName: string) => {
    const value = runtimeVars[varName];
    if (typeof value !== "string") {
      missingVars.add(varName);
      return full;
    }
    return value;
  });
  if (missingVars.size > 0) {
    const sortedMissing = [...missingVars].sort();
    throw new Error(`single-worker prompt runtime vars missing: ${sortedMissing.join(", ")}`);
  }
  const unresolved = [...rendered.matchAll(RUNTIME_PLACEHOLDER_PATTERN)].map((item) => item[1]);
  if (unresolved.length > 0) {
    throw new Error(
      `single-worker prompt has unresolved placeholders after render: ${[...new Set(unresolved)]
        .map((name) => `{${name}}`)
        .join(", ")}`,
    );
  }
  return rendered;
}

export async function resolveSingleWorkerProfilePrompt(params: {
  workspaceRoot: string;
  command: SingleWorkerCommand;
}): Promise<{ prompt: string; promptTemplatePath: string; stagePolicyPath: string }> {
  const workspaceRoot = path.resolve(assertNonEmptyString(params.workspaceRoot, "workspaceRoot"));
  const stagePolicyPath = path.join(workspaceRoot, "STAGES.yaml");
  const stagePolicyRaw = await fs.readFile(stagePolicyPath, "utf8");
  const stagePolicyRecord = asObjectRecord(
    YAML.parse(stagePolicyRaw, { schema: "core" }) as unknown,
    stagePolicyPath,
  );
  const stageName = asRequiredString(stagePolicyRecord.stage, `${stagePolicyPath}:stage`);
  if (stageName !== params.command.stage) {
    throw new Error(
      `single-worker stage mismatch: command.stage=${params.command.stage} stage_policy.stage=${stageName}`,
    );
  }
  const profiles = asObjectRecord(stagePolicyRecord.profiles, `${stagePolicyPath}:profiles`);
  const profileConfig = asObjectRecord(
    profiles[params.command.profile],
    `${stagePolicyPath}:profiles.${params.command.profile}`,
  );
  if (profileConfig.approved !== true) {
    const failure =
      typeof profileConfig.failure === "string" && profileConfig.failure.trim().length > 0
        ? `; ${profileConfig.failure.trim()}`
        : "";
    throw new Error(`single-worker profile is not approved: ${params.command.profile}${failure}`);
  }
  const promptRelativePath = asRequiredString(
    profileConfig.prompt,
    `${stagePolicyPath}:profiles.${params.command.profile}.prompt`,
  );
  const promptTemplatePath = resolvePromptTemplatePath({
    workspaceRoot,
    stagePolicyPath,
    promptRelativePath,
  });
  const promptTemplate = await fs.readFile(promptTemplatePath, "utf8");
  const prompt = renderSingleWorkerPromptTemplate(promptTemplate, params.command.runtime_vars);
  return {
    prompt,
    promptTemplatePath,
    stagePolicyPath,
  };
}

export function withRuntimeMarkers<TPayload extends object>(
  payload: TPayload,
  context: RuntimeContext,
): TPayload & { runtime_markers: RuntimeMarkers } {
  return {
    ...payload,
    runtime_markers: context.markers,
  };
}

export async function writeWorkspaceEvidence(params: {
  context: RuntimeContext;
  workspaceRoot: string;
  selectedStage: string;
}): Promise<string> {
  // workspace evidence 记录真实加载到的 agent 文件指纹，不用静态扫描冒充运行时事实。
  const workspaceRoot = path.resolve(assertNonEmptyString(params.workspaceRoot, "workspaceRoot"));
  const identityPath = path.join(workspaceRoot, "IDENTITY.md");
  const skillsManifestPath = path.join(workspaceRoot, "skills", "manifest.yaml");
  const stagePolicyPath = path.join(workspaceRoot, "STAGES.yaml");
  const evidence: WorkspaceEvidence = {
    source: "openclaw_workspace_loader",
    run_id: params.context.markers.run_id,
    call_id: params.context.markers.call_id,
    worker_id: params.context.markers.worker_id,
    stage: params.context.markers.stage,
    profile: params.context.markers.profile,
    openclaw_run_id: params.context.markers.openclaw_run_id,
    workspace_root: workspaceRoot,
    identity: {
      path: identityPath,
      sha256: await maybeSha256ForFile(identityPath),
    },
    skills_manifest: {
      path: skillsManifestPath,
      sha256: await maybeSha256ForFile(skillsManifestPath),
    },
    stage_policy: {
      path: stagePolicyPath,
      sha256: await maybeSha256ForFile(stagePolicyPath),
      selected_stage: params.selectedStage,
    },
    text_assets: await collectTextAssets(workspaceRoot),
    loaded_at: new Date().toISOString(),
  };
  const outputPath = path.join(params.context.evidenceDir, "workspace-evidence.json");
  await fs.writeFile(
    outputPath,
    `${JSON.stringify(withRuntimeMarkers(evidence, params.context), null, 2)}\n`,
    "utf8",
  );
  return outputPath;
}

function formatOpenVikingMaterialLine(ref: SingleWorkerMaterialReadRef): string {
  return [
    `worker=${ref.worker_id}`,
    `stage=${ref.stage}`,
    `material_id=${ref.material_id}`,
    "recommended_read=material_id+layer(L1)",
    `layer=L1`,
    `l1_sha256=${ref.l1_sha256}`,
    `l2_available=${ref.l2_allowed_prefix ? "yes" : "no"}`,
    `call_id=${ref.call_id}`,
  ].join(" | ");
}

export function renderOpenVikingMaterialBrief(command: SingleWorkerCommand): string {
  const lines: string[] = [OPENVIKING_MATERIAL_BRIEF_HEADER];
  if (command.upstream_materials.length === 0) {
    lines.push("- none");
  } else {
    for (const item of command.upstream_materials) {
      lines.push(`- ${formatOpenVikingMaterialLine(item)}`);
    }
  }
  return lines.join("\n");
}

export function renderOpenVikingWriteTargetBrief(command: SingleWorkerCommand): string {
  return [
    OPENVIKING_WRITE_TARGET_HEADER,
    `- run_id=${command.material_target.run_id}`,
    `- target_name=${command.material_target.target_name}`,
    `- uri=${command.material_target.l1_uri}`,
    `- worker_id=${command.material_target.worker_id}`,
    `- stage=${command.material_target.stage}`,
    `- call_id=${command.material_target.call_id}`,
  ].join("\n");
}

function readRuntimeVar(command: SingleWorkerCommand, key: string): string {
  const raw = command.runtime_vars[key];
  return typeof raw === "string" ? raw : "";
}

export function renderRuntimeTargetBrief(command: SingleWorkerCommand): string {
  return [
    RUNTIME_TARGET_HEADER,
    `- ticker=${readRuntimeVar(command, "ticker")}`,
    `- company_name=${readRuntimeVar(command, "company_name")}`,
    `- market=${readRuntimeVar(command, "market")}`,
    `- currency=${readRuntimeVar(command, "currency")}`,
    `- currency_symbol=${readRuntimeVar(command, "currency_symbol")}`,
    `- current_date=${readRuntimeVar(command, "current_date")}`,
    `- start_date=${readRuntimeVar(command, "start_date")}`,
    `- end_date=${readRuntimeVar(command, "end_date")}`,
  ].join("\n");
}

export function appendOpenVikingMaterialBrief(
  prompt: string,
  command: SingleWorkerCommand,
): string {
  // 追加给模型的是材料目录和写入目标，不是上游全文，避免 Python prompt stuffing。
  const base = stripRuntimeSections(prompt.trimEnd());
  const appendSections: string[] = [
    renderRuntimeTargetBrief(command),
    renderOpenVikingMaterialBrief(command),
    renderOpenVikingWriteTargetBrief(command),
  ];
  if (!base) {
    return appendSections.join("\n\n");
  }
  return `${base}\n\n${appendSections.join("\n\n")}`;
}

function stripRuntimeSections(text: string): string {
  const headers = new Set([
    RUNTIME_TARGET_HEADER,
    OPENVIKING_MATERIAL_BRIEF_HEADER,
    OPENVIKING_WRITE_TARGET_HEADER,
  ]);
  const lines = text.split(/\r?\n/g);
  const kept: string[] = [];
  let index = 0;
  while (index < lines.length) {
    const current = lines[index];
    if (!headers.has(current.trim())) {
      kept.push(current);
      index += 1;
      continue;
    }
    index += 1;
    while (index < lines.length) {
      const line = lines[index];
      const trimmed = line.trim();
      if (headers.has(trimmed)) {
        break;
      }
      if (trimmed === "" || trimmed.startsWith("-")) {
        index += 1;
        continue;
      }
      break;
    }
  }
  return kept.join("\n").trimEnd();
}
