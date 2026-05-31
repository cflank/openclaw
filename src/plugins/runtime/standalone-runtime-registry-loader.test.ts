import { beforeEach, describe, expect, it, vi } from "vitest";
import { createEmptyPluginRegistry } from "../registry-empty.js";
import type { PluginRegistry } from "../registry-types.js";
import {
  getActivePluginChannelRegistry,
  pinActivePluginChannelRegistry,
  resetPluginRuntimeStateForTest,
  setActivePluginRegistry,
} from "../runtime.js";

const mocks = vi.hoisted(() => ({
  loadOpenClawPlugins: vi.fn<typeof import("../loader.js").loadOpenClawPlugins>(),
  resolvePluginRegistryLoadCacheKey: vi.fn(() => "standalone-test-cache-key"),
}));

vi.mock("../loader.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../loader.js")>();
  return {
    ...actual,
    loadOpenClawPlugins: (...args: Parameters<typeof mocks.loadOpenClawPlugins>) =>
      mocks.loadOpenClawPlugins(...args),
    resolvePluginRegistryLoadCacheKey: (
      ...args: Parameters<typeof actual.resolvePluginRegistryLoadCacheKey>
    ) => mocks.resolvePluginRegistryLoadCacheKey(...args),
  };
});

function createChannelRegistry(pluginId: string): PluginRegistry {
  const registry = createEmptyPluginRegistry();
  registry.channels = [
    {
      pluginId,
      plugin: { id: pluginId, meta: {} },
      source: "test",
    },
  ] as never;
  return registry;
}

describe("standalone runtime channel registry loading", () => {
  beforeEach(() => {
    resetPluginRuntimeStateForTest();
    mocks.loadOpenClawPlugins.mockReset();
    mocks.resolvePluginRegistryLoadCacheKey.mockClear();
    mocks.resolvePluginRegistryLoadCacheKey.mockReturnValue("standalone-test-cache-key");
  });

  it("does not replace a pinned startup channel registry with a tool-only registry", async () => {
    const startup = createChannelRegistry("openclaw-weixin");
    setActivePluginRegistry(startup);
    pinActivePluginChannelRegistry(startup);

    const toolOnlyRegistry = createEmptyPluginRegistry();
    toolOnlyRegistry.tools = [
      {
        pluginId: "claw-trade-frontline-tools",
        factory: (() => []) as never,
        names: ["claw_trade_quote"],
        optional: false,
        source: "test",
      },
    ];
    mocks.loadOpenClawPlugins.mockReturnValue(toolOnlyRegistry);

    const { ensureStandaloneRuntimePluginRegistryLoaded } =
      await import("./standalone-runtime-registry-loader.js");

    ensureStandaloneRuntimePluginRegistryLoaded({
      surface: "channel",
      requiredPluginIds: ["claw-trade-frontline-tools"],
      loadOptions: {
        activate: false,
        toolDiscovery: true,
        onlyPluginIds: ["claw-trade-frontline-tools"],
      },
    });

    expect(mocks.loadOpenClawPlugins).toHaveBeenCalledTimes(1);
    expect(getActivePluginChannelRegistry()).toBe(startup);
  });

  it("allows a loaded registry with channels to replace the pinned channel registry", async () => {
    const startup = createChannelRegistry("openclaw-weixin");
    setActivePluginRegistry(startup);
    pinActivePluginChannelRegistry(startup);

    const replacement = createChannelRegistry("another-channel");
    mocks.loadOpenClawPlugins.mockReturnValue(replacement);

    const { ensureStandaloneRuntimePluginRegistryLoaded } =
      await import("./standalone-runtime-registry-loader.js");

    ensureStandaloneRuntimePluginRegistryLoaded({
      surface: "channel",
      requiredPluginIds: ["another-channel"],
      loadOptions: {
        activate: false,
        onlyPluginIds: ["another-channel"],
      },
    });

    expect(mocks.loadOpenClawPlugins).toHaveBeenCalledTimes(1);
    expect(getActivePluginChannelRegistry()).toBe(replacement);
  });
});
