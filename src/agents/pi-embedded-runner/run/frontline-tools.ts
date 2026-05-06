import { spawn } from "node:child_process";
import path from "node:path";
import { Type } from "@sinclair/typebox";
import type { AnyAgentTool } from "../../pi-tools.types.js";
import { payloadTextResult } from "../../tools/common.js";
import type { RuntimeContext } from "./params.js";

const FUNDAMENTALS_DATA_TOOL = "fundamentals_data";
const COMPANY_NEWS_TOOL = "company_news";
const MACRO_NEWS_TOOL = "macro_news";
const SOCIAL_SENTIMENT_TOOL = "social_sentiment";
const PYTHON_CANDIDATES = ["python3", "python"];
const DEFAULT_PYTHON_TOOL_TIMEOUT_MS = 120_000;
const DEFAULT_HTTP_TIMEOUT_MS = 20_000;

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
    package_name = f"_openclaw_frontline_runtime_{package_suffix}"
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

const TICKER_DATE_PARAMETERS = Type.Object(
  {
    ticker: Type.Optional(Type.String()),
    start_date: Type.Optional(Type.String()),
    end_date: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

const TICKER_ONLY_PARAMETERS = Type.Object(
  {
    ticker: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

const NOOP_PARAMETERS = Type.Object({}, { additionalProperties: false });

type ExecResult = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

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

function resolvePythonToolTimeoutMs(): number {
  const raw = process.env.OPENCLAW_FRONTLINE_TOOL_TIMEOUT_MS?.trim();
  if (!raw) {
    return DEFAULT_PYTHON_TOOL_TIMEOUT_MS;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_PYTHON_TOOL_TIMEOUT_MS;
  }
  return parsed;
}

function resolveHttpTimeoutMs(): number {
  const raw = process.env.OPENCLAW_FRONTLINE_HTTP_TIMEOUT_MS?.trim();
  if (!raw) {
    return DEFAULT_HTTP_TIMEOUT_MS;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_HTTP_TIMEOUT_MS;
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
    throw new Error("frontline python tool returned empty stdout");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(candidate);
  } catch (error) {
    throw new Error(
      `frontline python tool returned non-JSON payload: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("frontline python tool payload must be a JSON object");
  }
  return parsed as Record<string, unknown>;
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

async function execPythonJsonAllowingBusinessFailure(params: {
  toolName: string;
  entrypointPath: string;
  entrypointArgs: string[];
  workspaceRoot: string;
}): Promise<Record<string, unknown>> {
  const explicitPython = process.env.OPENCLAW_FRONTLINE_TOOL_PYTHON?.trim();
  const candidates = explicitPython ? [explicitPython] : PYTHON_CANDIDATES;
  const timeoutMs = resolvePythonToolTimeoutMs();
  let lastSpawnError: unknown;

  for (const pythonBin of candidates) {
    let result: ExecResult;
    try {
      result = await execPythonOnce({
        pythonBin,
        entrypointPath: params.entrypointPath,
        entrypointArgs: params.entrypointArgs,
        workspaceRoot: params.workspaceRoot,
        timeoutMs,
      });
    } catch (error) {
      const message =
        error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
      if (message.includes("enoent")) {
        lastSpawnError = error;
        continue;
      }
      return {
        ok: false,
        error: {
          type: "runtime_spawn_error",
          message: error instanceof Error ? error.message : String(error),
        },
      };
    }

    try {
      const payload = parseJsonObjectFromStdout(result.stdout);
      if (result.exitCode === 0) {
        return payload;
      }
      return {
        ...payload,
        ok: payload.ok === false ? false : false,
      };
    } catch {
      return {
        ok: false,
        error: {
          type: "entrypoint_failed",
          message:
            `${params.toolName} entrypoint failed: exit_code=${result.exitCode}; ` +
            `stderr=${summarizeTextForError(result.stderr)}; ` +
            `stdout=${summarizeTextForError(result.stdout)}`,
        },
      };
    }
  }

  return {
    ok: false,
    error: {
      type: "python_runtime_unavailable",
      message: `python runtime unavailable for frontline tools: ${
        lastSpawnError instanceof Error ? lastSpawnError.message : String(lastSpawnError)
      }`,
    },
  };
}

function resolveTicker(args: Record<string, unknown>, runtimeContext: RuntimeContext): string {
  const fromArgs = readOptionalString(args, "ticker");
  if (fromArgs) {
    return fromArgs;
  }
  const fromRuntime = runtimeContext.command.runtime_vars.ticker?.trim();
  if (fromRuntime) {
    return fromRuntime;
  }
  throw new Error("ticker is required");
}

function resolveDate(
  args: Record<string, unknown>,
  runtimeContext: RuntimeContext,
  key: "start_date" | "end_date",
): string | undefined {
  const fromArgs = readOptionalString(args, key);
  if (fromArgs) {
    return fromArgs;
  }
  const fromRuntime = runtimeContext.command.runtime_vars[key]?.trim();
  if (fromRuntime && fromRuntime.length > 0) {
    return fromRuntime;
  }
  return undefined;
}

async function fetchTextWithTimeout(url: string, timeoutMs: number): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "User-Agent": "openclaw-frontline-tool/1.0",
      },
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJsonWithTimeout(url: string, timeoutMs: number): Promise<unknown> {
  const text = await fetchTextWithTimeout(url, timeoutMs);
  try {
    return JSON.parse(text) as unknown;
  } catch (error) {
    throw new Error(
      `invalid JSON response: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function decodeXmlEntity(text: string): string {
  return text
    .replaceAll("&amp;", "&")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'");
}

function readXmlTag(itemXml: string, tag: string): string | undefined {
  const match = itemXml.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
  if (!match || !match[1]) {
    return undefined;
  }
  const value = decodeXmlEntity(match[1].replace(/<!\[CDATA\[|\]\]>/g, "").trim());
  return value.length > 0 ? value : undefined;
}

function parseRssItems(xmlText: string, maxItems: number): Array<Record<string, unknown>> {
  const itemMatches = [...xmlText.matchAll(/<item>([\s\S]*?)<\/item>/gi)];
  const items: Array<Record<string, unknown>> = [];
  for (const match of itemMatches) {
    const itemXml = match[1] ?? "";
    const title = readXmlTag(itemXml, "title");
    const link = readXmlTag(itemXml, "link");
    if (!title || !link) {
      continue;
    }
    items.push({
      title,
      link,
      published_at: readXmlTag(itemXml, "pubDate") ?? null,
      summary: readXmlTag(itemXml, "description") ?? null,
    });
    if (items.length >= maxItems) {
      break;
    }
  }
  return items;
}

function normalizeYahooNewsItems(raw: unknown, maxItems: number): Array<Record<string, unknown>> {
  if (!Array.isArray(raw)) {
    return [];
  }
  const items: Array<Record<string, unknown>> = [];
  for (const itemRaw of raw) {
    const record = asRecord(itemRaw);
    const title = readOptionalString(record, "title");
    const link = readOptionalString(record, "link");
    if (!title || !link) {
      continue;
    }
    items.push({
      title,
      link,
      published_at:
        readOptionalString(record, "providerPublishTime") ??
        readOptionalString(record, "pubDate") ??
        null,
      source: readOptionalString(record, "publisher") ?? "yahoo_finance",
      summary: readOptionalString(record, "summary") ?? null,
    });
    if (items.length >= maxItems) {
      break;
    }
  }
  return items;
}

function classifySentiment(text: string): "bullish" | "bearish" | "neutral" {
  const normalized = text.toLowerCase();
  const bullishWords = ["bull", "long", "buy", "breakout", "beat", "uptrend", "strong"];
  const bearishWords = ["bear", "short", "sell", "breakdown", "miss", "downtrend", "weak"];
  let bullish = 0;
  let bearish = 0;
  for (const word of bullishWords) {
    if (normalized.includes(word)) {
      bullish += 1;
    }
  }
  for (const word of bearishWords) {
    if (normalized.includes(word)) {
      bearish += 1;
    }
  }
  if (bullish === bearish) {
    return "neutral";
  }
  return bullish > bearish ? "bullish" : "bearish";
}

function resolveFundamentalsWorkspaceRoot(): string {
  const explicit = process.env.OPENCLAW_FUNDAMENTALS_WORKSPACE?.trim();
  if (explicit) {
    return path.resolve(explicit);
  }
  return path.resolve(process.cwd(), "agents", "market_analyst");
}

// 中文注释：这里只注册前线工具执行接缝，保持“模型自己调工具、失败返回结构化证据”的边界，不由 Python 预抓数据代写结论。
export function registerSingleWorkerFrontlineTools(runtimeContext: RuntimeContext): AnyAgentTool[] {
  const fundamentalsWorkspaceRoot = resolveFundamentalsWorkspaceRoot();
  const fundamentalsEntrypoint = path.join(
    fundamentalsWorkspaceRoot,
    "skills",
    "alphaear-stock",
    "scripts",
    "stock_entrypoint.py",
  );

  const fundamentalsTool: AnyAgentTool = {
    name: FUNDAMENTALS_DATA_TOOL,
    label: FUNDAMENTALS_DATA_TOOL,
    description: "Load fundamentals snapshot for the current ticker.",
    parameters: TICKER_ONLY_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const ticker = resolveTicker(args, runtimeContext);
      const payload = await execPythonJsonAllowingBusinessFailure({
        toolName: FUNDAMENTALS_DATA_TOOL,
        entrypointPath: fundamentalsEntrypoint,
        entrypointArgs: ["--skip-auto-update", "fundamentals", "--ticker", ticker],
        workspaceRoot: fundamentalsWorkspaceRoot,
      });
      return payloadTextResult({
        source: "frontline_tool_runtime",
        tool_name: FUNDAMENTALS_DATA_TOOL,
        ticker,
        ...payload,
      });
    },
  };

  const companyNewsTool: AnyAgentTool = {
    name: COMPANY_NEWS_TOOL,
    label: COMPANY_NEWS_TOOL,
    description: "Load company-specific news headlines for the ticker.",
    parameters: TICKER_DATE_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const ticker = resolveTicker(args, runtimeContext);
      const startDate = resolveDate(args, runtimeContext, "start_date");
      const endDate = resolveDate(args, runtimeContext, "end_date");
      const timeoutMs = resolveHttpTimeoutMs();
      try {
        const searchUrl = `https://query1.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(ticker)}&quotesCount=1&newsCount=12`;
        const searchPayload = asRecord(await fetchJsonWithTimeout(searchUrl, timeoutMs));
        const newsItems = normalizeYahooNewsItems(searchPayload.news, 12);
        if (newsItems.length === 0) {
          return payloadTextResult({
            source: "frontline_tool_runtime",
            tool_name: COMPANY_NEWS_TOOL,
            ok: false,
            ticker,
            start_date: startDate ?? null,
            end_date: endDate ?? null,
            limitations: [
              {
                type: "no_company_news_rows",
                message: `No company news rows returned for ticker ${ticker}.`,
                provider: "yahoo_finance_search",
              },
            ],
            articles: [],
          });
        }
        return payloadTextResult({
          source: "frontline_tool_runtime",
          tool_name: COMPANY_NEWS_TOOL,
          ok: true,
          ticker,
          start_date: startDate ?? null,
          end_date: endDate ?? null,
          provider: "yahoo_finance_search",
          articles: newsItems,
        });
      } catch (error) {
        return payloadTextResult({
          source: "frontline_tool_runtime",
          tool_name: COMPANY_NEWS_TOOL,
          ok: false,
          ticker,
          start_date: startDate ?? null,
          end_date: endDate ?? null,
          limitations: [
            {
              type: "provider_unavailable",
              provider: "yahoo_finance_search",
              message: error instanceof Error ? error.message : String(error),
            },
          ],
          articles: [],
        });
      }
    },
  };

  const macroNewsTool: AnyAgentTool = {
    name: MACRO_NEWS_TOOL,
    label: MACRO_NEWS_TOOL,
    description: "Load global macro news headlines for the current run window.",
    parameters: NOOP_PARAMETERS,
    execute: async () => {
      const timeoutMs = resolveHttpTimeoutMs();
      const rssFeeds = [
        { name: "reuters_world", url: "https://feeds.reuters.com/reuters/worldNews" },
        { name: "reuters_business", url: "https://feeds.reuters.com/reuters/businessNews" },
      ];
      const collected: Array<Record<string, unknown>> = [];
      const failures: Array<Record<string, unknown>> = [];
      for (const feed of rssFeeds) {
        try {
          const xml = await fetchTextWithTimeout(feed.url, timeoutMs);
          const items = parseRssItems(xml, 8).map((item) => ({
            ...item,
            source: feed.name,
          }));
          collected.push(...items);
        } catch (error) {
          failures.push({
            provider: feed.name,
            type: "provider_unavailable",
            message: error instanceof Error ? error.message : String(error),
          });
        }
      }
      if (collected.length === 0) {
        return payloadTextResult({
          source: "frontline_tool_runtime",
          tool_name: MACRO_NEWS_TOOL,
          ok: false,
          limitations:
            failures.length > 0
              ? failures
              : [{ type: "no_macro_news_rows", message: "No macro news rows returned." }],
          articles: [],
        });
      }
      return payloadTextResult({
        source: "frontline_tool_runtime",
        tool_name: MACRO_NEWS_TOOL,
        ok: true,
        articles: collected,
        provider_failures: failures,
      });
    },
  };

  const socialSentimentTool: AnyAgentTool = {
    name: SOCIAL_SENTIMENT_TOOL,
    label: SOCIAL_SENTIMENT_TOOL,
    description: "Load social chatter and sentiment snapshot for the ticker.",
    parameters: TICKER_ONLY_PARAMETERS,
    execute: async (_toolCallId, rawArgs) => {
      const args = asRecord(rawArgs);
      const ticker = resolveTicker(args, runtimeContext);
      const timeoutMs = resolveHttpTimeoutMs();
      const stocktwitsUrl = `https://api.stocktwits.com/api/2/streams/symbol/${encodeURIComponent(ticker)}.json`;
      try {
        const payload = asRecord(await fetchJsonWithTimeout(stocktwitsUrl, timeoutMs));
        const messagesRaw = Array.isArray(payload.messages) ? payload.messages : [];
        const messages: Array<Record<string, unknown>> = [];
        let bullish = 0;
        let bearish = 0;
        let neutral = 0;
        for (const raw of messagesRaw.slice(0, 25)) {
          const record = asRecord(raw);
          const body = readOptionalString(record, "body") ?? "";
          const sentiment = asRecord(asRecord(record.entities).sentiment).basic;
          const normalizedSentiment =
            typeof sentiment === "string" ? sentiment.toLowerCase() : classifySentiment(body);
          if (normalizedSentiment === "bullish") {
            bullish += 1;
          } else if (normalizedSentiment === "bearish") {
            bearish += 1;
          } else {
            neutral += 1;
          }
          messages.push({
            id: readOptionalString(record, "id") ?? null,
            created_at: readOptionalString(record, "created_at") ?? null,
            body,
            sentiment: normalizedSentiment,
            username: readOptionalString(asRecord(record.user), "username") ?? null,
          });
        }
        if (messages.length === 0) {
          return payloadTextResult({
            source: "frontline_tool_runtime",
            tool_name: SOCIAL_SENTIMENT_TOOL,
            ok: false,
            ticker,
            limitations: [
              {
                type: "no_social_rows",
                provider: "stocktwits",
                message: `No social messages returned for ticker ${ticker}.`,
              },
            ],
            messages: [],
          });
        }
        return payloadTextResult({
          source: "frontline_tool_runtime",
          tool_name: SOCIAL_SENTIMENT_TOOL,
          ok: true,
          ticker,
          provider: "stocktwits",
          summary: {
            bullish_count: bullish,
            bearish_count: bearish,
            neutral_count: neutral,
            sample_size: messages.length,
          },
          messages,
        });
      } catch (error) {
        return payloadTextResult({
          source: "frontline_tool_runtime",
          tool_name: SOCIAL_SENTIMENT_TOOL,
          ok: false,
          ticker,
          limitations: [
            {
              type: "provider_unavailable",
              provider: "stocktwits",
              message: error instanceof Error ? error.message : String(error),
            },
          ],
          messages: [],
        });
      }
    },
  };

  return [fundamentalsTool, companyNewsTool, macroNewsTool, socialSentimentTool];
}
