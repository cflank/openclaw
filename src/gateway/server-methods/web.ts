import { listChannelPlugins } from "../../channels/plugins/index.js";
import type { ChannelId } from "../../channels/plugins/types.public.js";
import { renderQrPngDataUrl } from "../../media/qr-image.js";
import {
  ErrorCodes,
  errorShape,
  formatValidationErrors,
  validateWebLoginStartParams,
  validateWebLoginWaitParams,
} from "../protocol/index.js";
import { formatForLog } from "../ws-log.js";
import type { GatewayRequestHandlers, RespondFn } from "./types.js";

const WEB_LOGIN_METHODS = new Set(["web.login.start", "web.login.wait"]);
const QR_PNG_DATA_URL_PREFIX = "data:image/png;base64,";

function channelPluginSupportsWebLoginMethod(
  plugin: ReturnType<typeof listChannelPlugins>[number],
  method: string,
): boolean {
  if ((plugin.gatewayMethods ?? []).some((candidate) => candidate === method)) {
    return true;
  }
  if (method === "web.login.start") {
    return Boolean(plugin.gateway?.loginWithQrStart);
  }
  if (method === "web.login.wait") {
    return Boolean(plugin.gateway?.loginWithQrWait);
  }
  return false;
}

const resolveWebLoginProvider = () =>
  listChannelPlugins().find((plugin) =>
    [...WEB_LOGIN_METHODS].some((method) => channelPluginSupportsWebLoginMethod(plugin, method)),
  ) ?? null;

function resolveAccountId(params: unknown): string | undefined {
  return typeof (params as { accountId?: unknown }).accountId === "string"
    ? (params as { accountId?: string }).accountId
    : undefined;
}

function respondProviderUnavailable(respond: RespondFn) {
  respond(
    false,
    undefined,
    errorShape(ErrorCodes.INVALID_REQUEST, "web login provider is not available"),
  );
}

function respondProviderUnsupported(respond: RespondFn, providerId: string) {
  respond(
    false,
    undefined,
    errorShape(ErrorCodes.INVALID_REQUEST, `web login is not supported by provider ${providerId}`),
  );
}

async function normalizeQrLoginResult<T>(result: T): Promise<T> {
  if (!result || typeof result !== "object") {
    return result;
  }
  const qrDataUrl = (result as { qrDataUrl?: unknown }).qrDataUrl;
  if (typeof qrDataUrl !== "string" || !qrDataUrl.trim()) {
    return result;
  }
  if (qrDataUrl.startsWith(QR_PNG_DATA_URL_PREFIX)) {
    return result;
  }
  return {
    ...result,
    qrDataUrl: await renderQrPngDataUrl(qrDataUrl),
  };
}

function wasChannelRunning(params: {
  context: Parameters<GatewayRequestHandlers["web.login.start"]>[0]["context"];
  channelId: ChannelId;
  accountId?: string;
}): boolean {
  const runtime = params.context.getRuntimeSnapshot();
  if (params.accountId) {
    const accountRuntime = runtime.channelAccounts[params.channelId]?.[params.accountId];
    if (accountRuntime) {
      return accountRuntime.running === true;
    }
  }
  if (!params.accountId) {
    return runtime.channels[params.channelId]?.running === true;
  }
  const defaultRuntime = runtime.channels[params.channelId];
  return defaultRuntime?.accountId === params.accountId && defaultRuntime.running === true;
}

export const webHandlers: GatewayRequestHandlers = {
  "web.login.start": async ({ params, respond, context }) => {
    if (!validateWebLoginStartParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid web.login.start params: ${formatValidationErrors(validateWebLoginStartParams.errors)}`,
        ),
      );
      return;
    }
    try {
      const accountId = resolveAccountId(params);
      const provider = resolveWebLoginProvider();
      if (!provider) {
        respondProviderUnavailable(respond);
        return;
      }
      if (!provider.gateway?.loginWithQrStart) {
        respondProviderUnsupported(respond, provider.id);
        return;
      }
      const wasRunning = wasChannelRunning({
        context,
        channelId: provider.id,
        accountId,
      });
      await context.stopChannel(provider.id, accountId);
      const result = await provider.gateway.loginWithQrStart({
        force: Boolean((params as { force?: boolean }).force),
        timeoutMs:
          typeof (params as { timeoutMs?: unknown }).timeoutMs === "number"
            ? (params as { timeoutMs?: number }).timeoutMs
            : undefined,
        verbose: Boolean((params as { verbose?: boolean }).verbose),
        accountId,
      });
      if (result.connected) {
        await context.startChannel(provider.id, accountId);
      } else if (wasRunning && !result.qrDataUrl) {
        await context.startChannel(provider.id, accountId);
      }
      respond(true, await normalizeQrLoginResult(result), undefined);
    } catch (err) {
      respond(false, undefined, errorShape(ErrorCodes.UNAVAILABLE, formatForLog(err)));
    }
  },
  "web.login.wait": async ({ params, respond, context }) => {
    if (!validateWebLoginWaitParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid web.login.wait params: ${formatValidationErrors(validateWebLoginWaitParams.errors)}`,
        ),
      );
      return;
    }
    try {
      const accountId = resolveAccountId(params);
      const provider = resolveWebLoginProvider();
      if (!provider) {
        respondProviderUnavailable(respond);
        return;
      }
      if (!provider.gateway?.loginWithQrWait) {
        respondProviderUnsupported(respond, provider.id);
        return;
      }
      const result = await provider.gateway.loginWithQrWait({
        timeoutMs:
          typeof (params as { timeoutMs?: unknown }).timeoutMs === "number"
            ? (params as { timeoutMs?: number }).timeoutMs
            : undefined,
        accountId,
        sessionKey:
          typeof (params as { sessionKey?: unknown }).sessionKey === "string"
            ? (params as { sessionKey?: string }).sessionKey
            : undefined,
        currentQrDataUrl:
          typeof (params as { currentQrDataUrl?: unknown }).currentQrDataUrl === "string"
            ? (params as { currentQrDataUrl?: string }).currentQrDataUrl
            : undefined,
      });
      if (result.connected) {
        await context.startChannel(provider.id, accountId);
      }
      respond(true, await normalizeQrLoginResult(result), undefined);
    } catch (err) {
      respond(false, undefined, errorShape(ErrorCodes.UNAVAILABLE, formatForLog(err)));
    }
  },
};
