import type { AgentTool } from "@mariozechner/pi-agent-core";
import type { RuntimeContext } from "./params.js";

export function buildSingleWorkerMinimalSystemContext(params: {
  context: RuntimeContext;
  tools: AgentTool[];
  workspaceDir: string;
}): string {
  void params;
  return "";
}
