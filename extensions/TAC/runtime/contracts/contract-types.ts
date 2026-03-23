export type ArtifactReleaseStatus = "draft" | "released";

export interface ContractArtifact {
  artifact_id: string;
  session_id: string;
  stage: string;
  as_of_ts: string;
  release_status: ArtifactReleaseStatus;
  release_id?: string;
  producer_agent?: string;
  consumer_agent?: string;
  payload: Record<string, unknown>;
}

export interface ContractRecord extends ContractArtifact {
  release_id: string;
  stored_at: string;
  stored_path: string;
}

