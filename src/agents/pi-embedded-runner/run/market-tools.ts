import { spawn } from "node:child_process";
import path from "node:path";
import { Type } from "@sinclair/typebox";
import type { AnyAgentTool } from "../../pi-tools.types.js";
import { payloadTextResult } from "../../tools/common.js";
import type { RuntimeContext } from "./params.js";

const STOCK_PRICE_TOOL = "market.stock_price";
const TECHLAB_ANALYZE_TOOL = "market.techlab_analyze";
const PYTHON_CANDIDATES = ["python3", "python"];
const DEFAULT_MARKET_TOOL_TIMEOUT_MS = 120_000;
const PYTHON_ENTRYPOINT_RUNNER = `
import importlib.util
import json
import pathlib
import re
import sys

def _load_runtime_package(package_name: str, scripts_dir: pathlib.Path):
    init_path = scripts_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        package_name,
        init_path,
        submodule_search_locations=[str(scripts_dir)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"runtime package load failed: {scripts_dir}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[package_name] = module
    spec.loader.exec_module(module)


def _load_entrypoint_module(module_name: str, entrypoint_path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(module_name, entrypoint_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"entrypoint module load failed: {entrypoint_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main():
    if len(sys.argv) != 3:
        raise RuntimeError("usage: <entrypoint_path> <json_argv>")
    entrypoint_path = pathlib.Path(sys.argv[1]).resolve()
    scripts_dir = entrypoint_path.parent
    skill_name = scripts_dir.parent.name
    package_suffix = re.sub(r"[^a-zA-Z0-9_]+", "_", skill_name)
    package_name = f"_openclaw_market_runtime_{package_suffix}"
    _load_runtime_package(package_name, scripts_dir)
    module_name = f"{package_name}.{entrypoint_path.stem}"
    module = _load_entrypoint_module(module_name, entrypoint_path)
    entrypoint_argv = json.loads(sys.argv[2])
    main_fn = getattr(module, "main", None)
    if not callable(main_fn):
        raise RuntimeError(f"entrypoint missing callable main: {entrypoint_path}")
    exit_code = main_fn(entrypoint_argv)
    raise SystemExit(0 if exit_code is None else int(exit_code))


if __name__ == "__main__":
    main()
`.trim();

const STOCK_PRICE_PARAMETERS = Type.Object(
  {
    ticker: Type.String(),
    start_date: Type.String(),
    end_date: Type.String(),
  },
  { additionalProperties: false },
);

const TECHLAB_ANALYZE_PARAMETERS = Type.Object(
  {
    ticker: Type.String(),
    start_date: Type.String(),
    end_date: Type.String(),
  },
  { additionalProperties: false },
);

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function readRequiredString(record: Record<string, unknown>, key: string): string {
  const raw = record[key];
  if (typeof raw !== "string") {
    throw new Error(`${key} is required`);
  }
  const value = raw.trim();
  if (!value) {
    throw new Error(`${key} is required`);
  }
  return value;
}

function sanitizeSegment(value: string): string {
  const normalized = value.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return normalized || "unknown";
}

function resolveMarketToolTimeoutMs(): number {
  const raw = process.env.OPENCLAW_MARKET_TOOL_TIMEOUT_MS?.trim();
  if (!raw) {
    return DEFAULT_MARKET_TOOL_TIMEOUT_MS;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_MARKET_TOOL_TIMEOUT_MS;
  }
  return parsed;
}

function parseJsonObjectFromStdout(stdout: string): Record<string, unknown> {
  const lines = stdout
    .split(/\r?\n/g)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const candidate = lines.length > 0 ? lines[lines.length - 1] : stdout.trim();
  if (!candidate) {
    throw new Error("market tool returned empty stdout");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(candidate);
  } catch (error) {
    throw new Error(
      `market tool returned non-JSON payload: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("market tool payload must be a JSON object");
  }
  return parsed as Record<string, unknown>;
}

type ExecResult = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

function summarizeTextForError(value: string): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "<empty>";
  }
  if (normalized.length <= 320) {
    return normalized;
  }
  return `${normalized.slice(0, 320)}...`;
}

async function execPythonOnce(params: {
  pythonBin: string;
  entrypointPath: string;
  entrypointArgs: string[];
  workspaceRoot: string;
  timeoutMs: number;
}): Promise<ExecResult> {
  return await new Promise<ExecResult>((resolve, reject) => {
    const argvPayload = JSON.stringify(params.entrypointArgs);
    const child = spawn(
      params.pythonBin,
      ["-c", PYTHON_ENTRYPOINT_RUNNER, params.entrypointPath, argvPayload],
      {
        cwd: params.workspaceRoot,
        stdio: ["ignore", "pipe", "pipe"],
        env: process.env,
      },
    );

    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        child.kill("SIGKILL");
      }
    }, params.timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.once("error", (error) => {
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (exitCode) => {
      settled = true;
      clearTimeout(timer);
      resolve({
        stdout,
        stderr,
        exitCode: typeof exitCode === "number" ? exitCode : 1,
      });
    });
  });
}

async function execMarketPythonJson(params: {
  toolName: string;
  entrypointPath: string;
  entrypointArgs: string[];
  workspaceRoot: string;
}): Promise<Record<string, unknown>> {
  const explicitPython = process.env.OPENCLAW_MARKET_TOOL_PYTHON?.trim();
  const candidates = explicitPython ? [explicitPython] : PYTHON_CANDIDATES;
  const timeoutMs = resolveMarketToolTimeoutMs();

  let lastSpawnError: unknown;
  let lastResult: ExecResult | undefined;
  for (const pythonBin of candidates) {
    try {
      lastResult = await execPythonOnce({
        pythonBin,
        entrypointPath: params.entrypointPath,
        entrypointArgs: params.entrypointArgs,
        workspaceRoot: params.workspaceRoot,
        timeoutMs,
      });
      break;
    } catch (error) {
      const message =
        error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
      if (message.includes("enoent")) {
        lastSpawnError = error;
        continue;
      }
      throw error;
    }
  }

  if (!lastResult) {
    throw new Error(
      `python runtime unavailable for market tools: ${lastSpawnError instanceof Error ? lastSpawnError.message : String(lastSpawnError)}`,
    );
  }

  if (lastResult.exitCode !== 0) {
    // 工具子进程非零退出必须按失败上抛，防止被记录成 success 并污染 tool-calls 证据。
    throw new Error(
      `${params.toolName} entrypoint failed: exit_code=${lastResult.exitCode}; ` +
        `stderr=${summarizeTextForError(lastResult.stderr)}; ` +
        `stdout=${summarizeTextForError(lastResult.stdout)}`,
    );
  }
  return parseJsonObjectFromStdout(lastResult.stdout);
}

function resolveWorkerWorkspaceRoot(
  runtimeContext: RuntimeContext,
  workerWorkspaceRoot?: string,
): string {
  if (workerWorkspaceRoot && workerWorkspaceRoot.trim().length > 0) {
    return path.resolve(workerWorkspaceRoot);
  }
  return path.resolve(process.cwd(), "agents", runtimeContext.command.worker_id);
}

type RegisterMarketToolsParams = {
  runtimeContext: RuntimeContext;
  workerWorkspaceRoot?: string;
};

// 该注册层只负责单 worker 的通用工具执行接缝：把模型 tool call 转到 worker workspace 下的技能脚本。
export function registerSingleWorkerMarketTools(params: RegisterMarketToolsParams): AnyAgentTool[] {
  const workspaceRoot = resolveWorkerWorkspaceRoot(
    params.runtimeContext,
    params.workerWorkspaceRoot,
  );
  const stockEntrypoint = path.join(
    workspaceRoot,
    "skills",
    "alphaear-stock",
    "scripts",
    "stock_entrypoint.py",
  );
  const techlabEntrypoint = path.join(
    workspaceRoot,
    "skills",
    "alphaear-techlab",
    "scripts",
    "techlab_entrypoint.py",
  );

  const stockPriceTool: AnyAgentTool = {
    name: STOCK_PRICE_TOOL,
    label: STOCK_PRICE_TOOL,
    description: "Load historical stock price rows for the current ticker and date range.",
    parameters: STOCK_PRICE_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const ticker = readRequiredString(args, "ticker");
      const startDate = readRequiredString(args, "start_date");
      const endDate = readRequiredString(args, "end_date");
      const payload = await execMarketPythonJson({
        toolName: STOCK_PRICE_TOOL,
        entrypointPath: stockEntrypoint,
        entrypointArgs: [
          "--skip-auto-update",
          "price",
          "--ticker",
          ticker,
          "--start-date",
          startDate,
          "--end-date",
          endDate,
        ],
        workspaceRoot,
      });
      return payloadTextResult({
        source: "market_skill_runtime",
        tool_name: STOCK_PRICE_TOOL,
        ...payload,
      });
    },
  };

  const techlabAnalyzeTool: AnyAgentTool = {
    name: TECHLAB_ANALYZE_TOOL,
    label: TECHLAB_ANALYZE_TOOL,
    description: "Run technical indicator analysis and chart generation for the current ticker.",
    parameters: TECHLAB_ANALYZE_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const ticker = readRequiredString(args, "ticker");
      const startDate = readRequiredString(args, "start_date");
      const endDate = readRequiredString(args, "end_date");
      const outputDir = path.join(
        "skills",
        "alphaear-techlab",
        "data",
        "charts",
        sanitizeSegment(params.runtimeContext.command.run_id),
        sanitizeSegment(params.runtimeContext.command.call_id),
      );
      const payload = await execMarketPythonJson({
        toolName: TECHLAB_ANALYZE_TOOL,
        entrypointPath: techlabEntrypoint,
        entrypointArgs: [
          "analyze",
          "--ticker",
          ticker,
          "--start-date",
          startDate,
          "--end-date",
          endDate,
          "--output-dir",
          outputDir,
        ],
        workspaceRoot,
      });
      return payloadTextResult({
        source: "market_skill_runtime",
        tool_name: TECHLAB_ANALYZE_TOOL,
        ...payload,
      });
    },
  };

  return [stockPriceTool, techlabAnalyzeTool];
}
