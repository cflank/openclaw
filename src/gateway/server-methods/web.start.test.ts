import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ChannelRuntimeSnapshot } from "../server-channel-runtime.types.js";
import type { GatewayRequestHandlerOptions } from "./types.js";

const mocks = vi.hoisted(() => ({
  listChannelPlugins: vi.fn(),
  renderQrPngDataUrl: vi.fn(),
}));

vi.mock("../../channels/plugins/index.js", () => ({
  listChannelPlugins: mocks.listChannelPlugins,
}));

vi.mock("../../media/qr-image.js", () => ({
  renderQrPngDataUrl: mocks.renderQrPngDataUrl,
}));

import { webHandlers } from "./web.js";

function createOptions(
  params: Record<string, unknown>,
  overrides?: Partial<GatewayRequestHandlerOptions>,
): GatewayRequestHandlerOptions {
  return {
    req: { type: "req", id: "req-1", method: "web.login.start", params },
    params,
    client: null,
    isWebchatConnect: () => false,
    respond: vi.fn(),
    context: {
      stopChannel: vi.fn(),
      startChannel: vi.fn(),
      getRuntimeSnapshot: vi.fn(
        (): ChannelRuntimeSnapshot => ({
          channels: {
            whatsapp: {
              accountId: "default",
              running: true,
            },
          },
          channelAccounts: {
            whatsapp: {
              default: {
                accountId: "default",
                running: true,
              },
            },
          },
        }),
      ),
    },
    ...overrides,
  } as unknown as GatewayRequestHandlerOptions;
}

function createRunningWhatsappContext() {
  const startChannel = vi.fn();
  const stopChannel = vi.fn();
  return {
    startChannel,
    stopChannel,
    context: {
      stopChannel,
      startChannel,
      getRuntimeSnapshot: vi.fn(
        (): ChannelRuntimeSnapshot => ({
          channels: {
            whatsapp: {
              accountId: "default",
              running: true,
            },
          },
          channelAccounts: {
            whatsapp: {
              default: {
                accountId: "default",
                running: true,
              },
            },
          },
        }),
      ),
    } as unknown as GatewayRequestHandlerOptions["context"],
  };
}

describe("webHandlers web.login.start", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.renderQrPngDataUrl.mockResolvedValue("data:image/png;base64,rendered-qr");
  });

  it("restarts a previously running channel when login start exits early without a QR", async () => {
    const loginWithQrStart = vi.fn().mockResolvedValue({
      code: "whatsapp-auth-unstable",
      message: "retry later",
    });
    mocks.listChannelPlugins.mockReturnValue([
      {
        id: "whatsapp",
        gatewayMethods: ["web.login.start"],
        gateway: { loginWithQrStart },
      },
    ]);
    const { context, startChannel, stopChannel } = createRunningWhatsappContext();
    const respond = vi.fn();

    await webHandlers["web.login.start"](
      createOptions(
        { accountId: "default" },
        {
          respond,
          context,
        },
      ),
    );

    expect(stopChannel).toHaveBeenCalledWith("whatsapp", "default");
    expect(startChannel).toHaveBeenCalledWith("whatsapp", "default");
    expect(respond).toHaveBeenCalledWith(
      true,
      {
        code: "whatsapp-auth-unstable",
        message: "retry later",
      },
      undefined,
    );
  });

  it("keeps the channel stopped when login start has taken over with a QR flow", async () => {
    const loginWithQrStart = vi.fn().mockResolvedValue({
      qrDataUrl: "data:image/png;base64,qr",
      message: "scan qr",
    });
    mocks.listChannelPlugins.mockReturnValue([
      {
        id: "whatsapp",
        gatewayMethods: ["web.login.start"],
        gateway: { loginWithQrStart },
      },
    ]);
    const { context, startChannel, stopChannel } = createRunningWhatsappContext();

    await webHandlers["web.login.start"](
      createOptions(
        { accountId: "default" },
        {
          context,
        },
      ),
    );

    expect(stopChannel).toHaveBeenCalledWith("whatsapp", "default");
    expect(startChannel).not.toHaveBeenCalled();
  });

  it("uses and renders channel QR login providers even when they do not declare gateway method names", async () => {
    const loginWithQrStart = vi.fn().mockResolvedValue({
      qrDataUrl: "https://liteapp.weixin.qq.com/q/weixin-qr",
      message: "scan qr",
      sessionKey: "weixin-session-1",
    });
    mocks.listChannelPlugins.mockReturnValue([
      {
        id: "openclaw-weixin",
        gatewayMethods: [],
        gateway: { loginWithQrStart },
      },
    ]);
    const respond = vi.fn();

    await webHandlers["web.login.start"](
      createOptions(
        { force: true, timeoutMs: 12000 },
        {
          respond,
        },
      ),
    );

    expect(loginWithQrStart).toHaveBeenCalledWith({
      accountId: undefined,
      force: true,
      timeoutMs: 12000,
      verbose: false,
    });
    expect(mocks.renderQrPngDataUrl).toHaveBeenCalledWith(
      "https://liteapp.weixin.qq.com/q/weixin-qr",
    );
    expect(respond).toHaveBeenCalledWith(
      true,
      {
        qrDataUrl: "data:image/png;base64,rendered-qr",
        message: "scan qr",
        sessionKey: "weixin-session-1",
      },
      undefined,
    );
  });
});

describe("webHandlers web.login.wait", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.renderQrPngDataUrl.mockResolvedValue("data:image/png;base64,rendered-qr");
  });

  it("passes refreshed QR payloads back to the client while login is still pending", async () => {
    const loginWithQrWait = vi.fn().mockResolvedValue({
      connected: false,
      message: "QR refreshed. Scan the latest code in WhatsApp → Linked Devices.",
      qrDataUrl: "data:image/png;base64,next-qr",
    });
    mocks.listChannelPlugins.mockReturnValue([
      {
        id: "whatsapp",
        gatewayMethods: ["web.login.wait"],
        gateway: { loginWithQrWait },
      },
    ]);
    const respond = vi.fn();

    await webHandlers["web.login.wait"](
      createOptions(
        {
          accountId: "default",
          sessionKey: "login-session-1",
          timeoutMs: 5000,
          currentQrDataUrl: "data:image/png;base64,current-qr",
        },
        {
          req: {
            type: "req",
            id: "req-2",
            method: "web.login.wait",
            params: {
              accountId: "default",
              sessionKey: "login-session-1",
              timeoutMs: 5000,
              currentQrDataUrl: "data:image/png;base64,current-qr",
            },
          } as GatewayRequestHandlerOptions["req"],
          respond,
        },
      ),
    );

    expect(loginWithQrWait).toHaveBeenCalledWith({
      accountId: "default",
      sessionKey: "login-session-1",
      timeoutMs: 5000,
      currentQrDataUrl: "data:image/png;base64,current-qr",
    });
    expect(respond).toHaveBeenCalledWith(
      true,
      {
        connected: false,
        message: "QR refreshed. Scan the latest code in WhatsApp → Linked Devices.",
        qrDataUrl: "data:image/png;base64,next-qr",
      },
      undefined,
    );
  });

  it("uses and renders channel QR wait providers even when they do not declare gateway method names", async () => {
    const loginWithQrWait = vi.fn().mockResolvedValue({
      connected: false,
      message: "scan latest qr",
      qrDataUrl: "https://liteapp.weixin.qq.com/q/weixin-next-qr",
    });
    mocks.listChannelPlugins.mockReturnValue([
      {
        id: "openclaw-weixin",
        gatewayMethods: [],
        gateway: { loginWithQrWait },
      },
    ]);
    const respond = vi.fn();

    await webHandlers["web.login.wait"](
      createOptions(
        {
          timeoutMs: 1500,
          sessionKey: "weixin-session-2",
          currentQrDataUrl: "data:image/png;base64,weixin-current-qr",
        },
        {
          req: {
            type: "req",
            id: "req-3",
            method: "web.login.wait",
            params: {
              timeoutMs: 1500,
              sessionKey: "weixin-session-2",
              currentQrDataUrl: "data:image/png;base64,weixin-current-qr",
            },
          } as GatewayRequestHandlerOptions["req"],
          respond,
        },
      ),
    );

    expect(loginWithQrWait).toHaveBeenCalledWith({
      accountId: undefined,
      sessionKey: "weixin-session-2",
      timeoutMs: 1500,
      currentQrDataUrl: "data:image/png;base64,weixin-current-qr",
    });
    expect(mocks.renderQrPngDataUrl).toHaveBeenCalledWith(
      "https://liteapp.weixin.qq.com/q/weixin-next-qr",
    );
    expect(respond).toHaveBeenCalledWith(
      true,
      {
        connected: false,
        message: "scan latest qr",
        qrDataUrl: "data:image/png;base64,rendered-qr",
      },
      undefined,
    );
  });
});
