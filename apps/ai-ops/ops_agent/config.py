"""Settings for the agent (ops-agent) and the approval service (ops-control), from the environment."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _csv(name: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in os.environ.get(name, "").split(",") if v.strip())


@dataclass(frozen=True)
class AgentConfig:
    # Claude in Amazon Bedrock (InvokeModel) through the US cross-region inference profiles.
    model: str = "us.anthropic.claude-sonnet-4-6"
    # Used only if the primary model declines a request (stop_reason "refusal").
    fallback_model: str = "us.anthropic.claude-opus-5-5"
    aws_region: str = "us-east-1"
    effort: str = "medium"
    max_tokens: int = 16_000
    # Hard limits per investigation; the model is told when it reaches them and must answer.
    max_turns: int = 16
    max_tool_calls: int = 24
    mcp_url: str = "http://mcp-platform-tools.ai-ops.svc:8080/mcp"
    mcp_token: str = field(default="", repr=False)
    control_url: str = "http://ops-control.ai-ops.svc:8080"
    control_token: str = field(default="", repr=False)
    # Webhook guardrails: at most this many investigations per hour, one per alert group per window.
    max_investigations_per_hour: int = 6
    dedupe_minutes: int = 30
    severities: tuple[str, ...] = ("critical", "warning")
    # AWS: forward each investigation to the AgentCore Runtime agent instead of running it in the pod.
    agent_runtime_arn: str = ""
    gateway_url: str = ""          # the runtime's MCP endpoint (AgentCore Gateway, SigV4)

    @classmethod
    def from_env(cls) -> AgentConfig:
        d = cls()
        return cls(
            model=_env("MODEL_ID", d.model), fallback_model=_env("FALLBACK_MODEL_ID", d.fallback_model),
            aws_region=_env("AWS_REGION", d.aws_region), effort=_env("EFFORT", d.effort),
            max_tokens=int(_env("MAX_TOKENS", str(d.max_tokens))),
            max_turns=int(_env("MAX_TURNS", str(d.max_turns))),
            max_tool_calls=int(_env("MAX_TOOL_CALLS", str(d.max_tool_calls))),
            mcp_url=_env("MCP_URL", d.mcp_url), mcp_token=_env("MCP_TOKEN", ""),
            control_url=_env("CONTROL_URL", d.control_url), control_token=_env("CONTROL_TOKEN", ""),
            max_investigations_per_hour=int(_env("MAX_INVESTIGATIONS_PER_HOUR", str(d.max_investigations_per_hour))),
            dedupe_minutes=int(_env("DEDUPE_MINUTES", str(d.dedupe_minutes))),
            severities=_csv("SEVERITIES") or d.severities,
            agent_runtime_arn=_env("AGENT_RUNTIME_ARN", ""), gateway_url=_env("GATEWAY_URL", ""),
        )


@dataclass(frozen=True)
class ControlConfig:
    database_url: str = field(default="", repr=False)
    control_token: str = field(default="", repr=False)
    # Key for the single-use execution tokens (HMAC-SHA256). Random per installation.
    signing_key: str = field(default="", repr=False)
    # Slack user IDs allowed to approve. Empty: nobody can approve from Slack (CLI only).
    approvers: tuple[str, ...] = ()
    required_approvals: int = 1
    approval_ttl_minutes: int = 30
    # Dry run: approvals are recorded and the action is logged, but nothing is changed in the cluster.
    dry_run: bool = False
    slack_bot_token: str = field(default="", repr=False)
    slack_app_token: str = field(default="", repr=False)
    slack_channel: str = ""
    grafana_url: str = "http://localhost:3000"

    @classmethod
    def from_env(cls) -> ControlConfig:
        d = cls()
        return cls(
            database_url=_env("DATABASE_URL", ""), control_token=_env("CONTROL_TOKEN", ""),
            signing_key=_env("ACTION_SIGNING_KEY", ""), approvers=_csv("APPROVERS"),
            required_approvals=int(_env("REQUIRED_APPROVALS", str(d.required_approvals))),
            approval_ttl_minutes=int(_env("APPROVAL_TTL_MINUTES", str(d.approval_ttl_minutes))),
            dry_run=_env("DRY_RUN", "false").lower() == "true",
            slack_bot_token=_env("SLACK_BOT_TOKEN", ""), slack_app_token=_env("SLACK_APP_TOKEN", ""),
            slack_channel=_env("SLACK_CHANNEL", ""), grafana_url=_env("GRAFANA_URL", d.grafana_url),
        )
