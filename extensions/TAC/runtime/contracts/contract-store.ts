import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ContractArtifact, ContractRecord } from "./contract-types.js";
import { resolveTacContractsDir } from "../shared/paths.js";

export interface ContractStoreOptions {
  contractsDir?: string;
}

function ensureDir(p: string): void {
  fs.mkdirSync(p, { recursive: true });
}

function readJson<T>(filePath: string): T {
  return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
}

export function writeReleasedArtifact(
  artifact: ContractArtifact,
  options: ContractStoreOptions = {},
): ContractRecord {
  if (artifact.release_status !== "released") {
    throw new Error("writeReleasedArtifact requires release_status=released");
  }
  const contractsDir = resolveTacContractsDir(options.contractsDir);
  const releaseId = artifact.release_id ?? randomUUID();
  const dir = path.join(contractsDir, artifact.session_id, artifact.artifact_id);
  ensureDir(dir);
  const filePath = path.join(dir, `${releaseId}.json`);
  const storedAt = new Date().toISOString();
  const record: ContractRecord = {
    ...artifact,
    release_id: releaseId,
    stored_at: storedAt,
    stored_path: filePath,
  };
  fs.writeFileSync(filePath, JSON.stringify(record, null, 2), "utf8");
  return record;
}

export function readLatestReleasedArtifact(
  sessionId: string,
  artifactId: string,
  options: ContractStoreOptions = {},
): ContractRecord | null {
  const contractsDir = resolveTacContractsDir(options.contractsDir);
  const dir = path.join(contractsDir, sessionId, artifactId);
  if (!fs.existsSync(dir)) {
    return null;
  }
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => path.join(dir, f));
  if (files.length === 0) {
    return null;
  }
  files.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return readJson<ContractRecord>(files[0]!);
}

export function hasReleasedArtifact(
  sessionId: string,
  artifactId: string,
  options: ContractStoreOptions = {},
): boolean {
  return readLatestReleasedArtifact(sessionId, artifactId, options) !== null;
}

