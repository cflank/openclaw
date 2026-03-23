import fs from "node:fs";
import Ajv2020 from "ajv/dist/2020.js";
import { resolveRiskVerdictSchemaPath } from "../shared/paths.js";

export type ArtifactReleaseStatus = "draft" | "released";
export type ValidationAction = "release" | "handoff" | "publish" | "risk_gate";

export interface ArtifactReference {
  artifact_id: string;
  released: boolean;
}

export interface ArtifactPayload {
  artifact_id: string;
  session_id: string;
  stage: string;
  as_of_ts: string;
  release_status: ArtifactReleaseStatus;
  producer_agent?: string;
  consumer_agent?: string;
  [key: string]: unknown;
}

export interface ReleaseValidationInput {
  artifact: ArtifactPayload;
  action?: ValidationAction;
  references?: ArtifactReference[];
}

export interface ReleaseValidationOptions {
  schemaPath?: string;
  schemaObject?: Record<string, unknown>;
}

export interface ReleaseValidationResult {
  allowed: boolean;
  reason_code: string;
  mandatory_audit_event?: string;
  errors: string[];
  schema_valid: boolean;
}

let cachedSchemaPath: string | null = null;
let cachedValidator: ((data: unknown) => boolean) | null = null;

function buildAjvValidator(schema: Record<string, unknown>) {
  const ajv = new Ajv2020({ allErrors: true, strict: false, validateFormats: false });
  return ajv.compile(schema);
}

function getRiskVerdictValidator(options: ReleaseValidationOptions): (data: unknown) => boolean {
  if (options.schemaObject) {
    return buildAjvValidator(options.schemaObject);
  }
  const p = resolveRiskVerdictSchemaPath(options.schemaPath);
  if (cachedValidator && cachedSchemaPath === p) {
    return cachedValidator;
  }
  const schema = JSON.parse(fs.readFileSync(p, "utf8")) as Record<string, unknown>;
  cachedValidator = buildAjvValidator(schema);
  cachedSchemaPath = p;
  return cachedValidator;
}

function validateBaseFields(artifact: ArtifactPayload, errors: string[]): void {
  const required: Array<keyof ArtifactPayload> = [
    "artifact_id",
    "session_id",
    "stage",
    "as_of_ts",
    "release_status",
  ];
  for (const key of required) {
    const val = artifact[key];
    if (typeof val !== "string" || val.length === 0) {
      errors.push(`missing_or_invalid:${key}`);
    }
  }
}

export function validateRelease(
  input: ReleaseValidationInput,
  options: ReleaseValidationOptions = {},
): ReleaseValidationResult {
  const errors: string[] = [];
  validateBaseFields(input.artifact, errors);
  let schemaValid = true;

  for (const ref of input.references ?? []) {
    if (!ref.released) {
      errors.push(`reference_not_released:${ref.artifact_id}`);
    }
  }

  if (input.action === "handoff") {
    const crossAgent =
      Boolean(input.artifact.producer_agent) &&
      Boolean(input.artifact.consumer_agent) &&
      input.artifact.producer_agent !== input.artifact.consumer_agent;
    if (crossAgent && input.artifact.release_status !== "released") {
      errors.push("cross_agent_handoff_requires_released");
      return {
        allowed: false,
        reason_code: "non_released_handoff",
        mandatory_audit_event: "NON_RELEASED_HANDOFF_BLOCKED",
        errors,
        schema_valid: schemaValid,
      };
    }
  }

  if (input.action === "publish" && input.artifact.artifact_id === "final_report") {
    if (input.artifact.release_status !== "released") {
      errors.push("final_report_not_released");
      return {
        allowed: false,
        reason_code: "publish_gate_blocked",
        mandatory_audit_event: "PUBLISH_GATE_BLOCKED",
        errors,
        schema_valid: schemaValid,
      };
    }
  }

  if (input.artifact.artifact_id === "risk_verdict") {
    const validate = getRiskVerdictValidator(options);
    const schemaTarget =
      typeof input.artifact.payload === "object" && input.artifact.payload !== null
        ? input.artifact.payload
        : input.artifact;
    schemaValid = validate(schemaTarget);
    if (!schemaValid) {
      const vErrors = (validate as { errors?: Array<{ message?: string; instancePath?: string }> }).errors ?? [];
      for (const ve of vErrors) {
        errors.push(`schema_error:${ve.instancePath || "/"}:${ve.message || "invalid"}`);
      }
      return {
        allowed: false,
        reason_code: "risk_verdict_schema_invalid",
        mandatory_audit_event: "CRITICAL_DATA_UNAVAILABLE_BLOCKED",
        errors,
        schema_valid: false,
      };
    }
  }

  if (errors.length > 0) {
    return {
      allowed: false,
      reason_code: "release_validation_failed",
      mandatory_audit_event: "NON_RELEASED_HANDOFF_BLOCKED",
      errors,
      schema_valid: schemaValid,
    };
  }

  return {
    allowed: true,
    reason_code: "release_valid",
    errors: [],
    schema_valid: schemaValid,
  };
}
