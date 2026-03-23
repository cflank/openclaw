# Tool Boundary
- Allowed MCP: market-cn-mcp, market-us-mcp, market-crypto-mcp
- Allowed Skills: acp-router, fallback-chain-orchestrator
- Forbidden: get_event_window and get_news_digest unless owner is market-news-mcp
- Non-released cross-agent handoff must be blocked with NON_RELEASED_HANDOFF_BLOCKED
- final_report publish when not released must be blocked with PUBLISH_GATE_BLOCKED

