"""The model client: Claude in Amazon Bedrock through the Anthropic SDK's Mantle client; a refusal is retried once on the fallback."""
from __future__ import annotations

from typing import Any

from ops_agent.config import AgentConfig


def bedrock_client(config: AgentConfig) -> Any:
    from anthropic import AsyncAnthropicBedrockMantle, BetaRefusalFallbackMiddleware

    return AsyncAnthropicBedrockMantle(
        aws_region=config.aws_region,
        max_retries=3,
        timeout=180.0,
        middleware=[BetaRefusalFallbackMiddleware([{"model": config.fallback_model}])],
    )
