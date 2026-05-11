import type { AgentTool } from "@mariozechner/pi-agent-core";
import type { RuntimeContext } from "./params.js";

export function buildSingleWorkerMinimalSystemContext(params: {
  context: RuntimeContext;
  tools: AgentTool[];
  workspaceDir: string;
}): string {
  void params;
  return [
    "你正在执行当前分析师的一轮任务。",
    "请按用户消息中的角色、工具和报告格式要求完成本轮分析。",
    "只能依据本轮可见工具结果写报告，不要编造工具没有返回的数据或来源。",
  ].join("\n");
}
