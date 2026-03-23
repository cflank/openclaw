import fs from "node:fs";
import { resolveTacPolicyPath } from "../shared/paths.js";

export type TacAction = "mcp_call" | "tool_call" | "skill_call" | "handoff" | "publish";

export interface PolicyEvaluationInput {
  agentId: string;
  stage: string;
  action: TacAction;
  ownerId?: string;
  toolName?: string;
  skillId?: string;
  allowedSkills?: string[];
  artifactId?: string;
  artifactReleased?: boolean;
  finalReportReleased?: boolean;
}

interface OwnerRule {
  owner_id: string;
  allowed_tools: string[];
  forbidden_tools: string[];
}

interface PolicyConfig {
  fail_closed_default: boolean;
  events: {
    owner_violation: string;
    tool_owner_mismatch: string;
    skill_denied: string;
    non_released_handoff: string;
    publish_gate_blocked: string;
  };
  global_rules: {
    news_owner_exclusive_tools: string[];
    block_non_released_cross_agent_handoff: boolean;
    block_publish_when_final_report_not_released: boolean;
  };
  owners: OwnerRule[];
  skill_constraints: {
    deny_unknown_skills: boolean;
  };
}

export interface PolicyEvaluationOptions {
  policyPath?: string;
  policy?: PolicyConfig;
}

export interface PolicyEvaluationResult {
  decision: "allow" | "deny";
  reason_code: string;
  mandatory_audit_event?: string;
  fail_closed: boolean;
  metadata: Record<string, unknown>;
}

let cachedPath: string | null = null;
let cachedPolicy: PolicyConfig | null = null;

function loadPolicy(pathOverride?: string): PolicyConfig {
  const p = resolveTacPolicyPath(pathOverride);
  if (cachedPolicy && cachedPath === p) {
    return cachedPolicy;
  }
  const raw = fs.readFileSync(p, "utf8");
  const parsed = JSON.parse(raw) as PolicyConfig;
  cachedPath = p;
  cachedPolicy = parsed;
  return parsed;
}

function deny(
  policy: PolicyConfig,
  reasonCode: string,
  auditEvent: string,
  metadata: Record<string, unknown> = {},
): PolicyEvaluationResult {
  return {
    decision: "deny",
    reason_code: reasonCode,
    mandatory_audit_event: auditEvent,
    fail_closed: policy.fail_closed_default,
    metadata,
  };
}

function allow(policy: PolicyConfig, metadata: Record<string, unknown> = {}): PolicyEvaluationResult {
  return {
    decision: "allow",
    reason_code: "allowed",
    fail_closed: policy.fail_closed_default,
    metadata,
  };
}

export function evaluatePolicy(
  input: PolicyEvaluationInput,
  options: PolicyEvaluationOptions = {},
): PolicyEvaluationResult {
  const policy = options.policy ?? loadPolicy(options.policyPath);
  const ownerRules = new Map(policy.owners.map((o) => [o.owner_id, o]));
  const newsExclusiveTools = new Set(policy.global_rules.news_owner_exclusive_tools);

  if (
    input.action === "handoff" &&
    policy.global_rules.block_non_released_cross_agent_handoff &&
    input.artifactReleased === false
  ) {
    return deny(policy, "non_released_handoff", policy.events.non_released_handoff, {
      artifact_id: input.artifactId ?? null,
    });
  }

  if (
    input.action === "publish" &&
    policy.global_rules.block_publish_when_final_report_not_released &&
    input.finalReportReleased === false
  ) {
    return deny(policy, "publish_gate_blocked", policy.events.publish_gate_blocked);
  }

  if (input.action === "skill_call" && input.skillId) {
    const allowedSkills = input.allowedSkills ?? [];
    if (!allowedSkills.includes(input.skillId) && policy.skill_constraints.deny_unknown_skills) {
      return deny(policy, "skill_denied", policy.events.skill_denied, { skill_id: input.skillId });
    }
  }

  if ((input.action === "tool_call" || input.action === "mcp_call") && input.ownerId && input.toolName) {
    const owner = ownerRules.get(input.ownerId);
    if (!owner) {
      return deny(policy, "owner_violation", policy.events.owner_violation, {
        owner_id: input.ownerId,
        tool_name: input.toolName,
      });
    }

    if (newsExclusiveTools.has(input.toolName) && input.ownerId !== "market-news-mcp") {
      return deny(policy, "tool_owner_mismatch", policy.events.tool_owner_mismatch, {
        owner_id: input.ownerId,
        tool_name: input.toolName,
      });
    }

    if (owner.forbidden_tools.includes(input.toolName)) {
      return deny(policy, "tool_owner_mismatch", policy.events.tool_owner_mismatch, {
        owner_id: input.ownerId,
        tool_name: input.toolName,
      });
    }

    if (!owner.allowed_tools.includes(input.toolName) && !owner.forbidden_tools.includes(input.toolName)) {
      return deny(policy, "owner_violation", policy.events.owner_violation, {
        owner_id: input.ownerId,
        tool_name: input.toolName,
      });
    }
  }

  return allow(policy);
}
