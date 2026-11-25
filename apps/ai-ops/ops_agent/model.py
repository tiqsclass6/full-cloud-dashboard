"""The model client: Claude in Amazon Bedrock (InvokeModel) through the Anthropic SDK; a refusal is retried once on the fallback."""
from __future__ import annotations

from typing import Any

from ops_agent.config import AgentConfig


def bedrock_client(config: AgentConfig) -> Any:
    from anthropic import AsyncAnthropicBedrock, BetaRefusalFallbackMiddleware

    # InvokeModel, not bedrock-mantle: Mantle does not serve Sonnet 4.6 in this account (docs/ai-ops.md).
    return AsyncAnthropicBedrock(
        aws_region=config.aws_region,
        max_retries=3,
        timeout=180.0,
        middleware=[BetaRefusalFallbackMiddleware([{"model": config.fallback_model}])],
    )
