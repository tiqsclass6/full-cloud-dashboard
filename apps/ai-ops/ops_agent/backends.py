"""Where tool calls go: the live MCP server, or a recorded fixture (the eval suite's replay mode)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx2 as httpx


@dataclass
class ToolResult:
    text: str
    is_error: bool = False
    replay_miss: bool = False      # replay only: no recorded result matched the call


class ToolBackend(Protocol):
    async def list_tools(self) -> list[dict[str, Any]]: ...
    async def call(self, name: str, arguments: dict[str, Any]) -> ToolResult: ...


def canonical(arguments: dict[str, Any]) -> str:
    return json.dumps(arguments, sort_keys=True, separators=(",", ":"))


GATEWAY_SEPARATOR = "___"


class SigV4Auth(httpx.Auth):
    """Signs each request with AWS SigV4 (AgentCore Gateway with the AWS_IAM authorizer)."""

    requires_request_body = True

    def __init__(self, region: str, service: str = "bedrock-agentcore") -> None:
        import boto3

        self._credentials = boto3.session.Session().get_credentials()
        self._region, self._service = region, service

    def auth_flow(self, request: httpx.Request):
        from botocore.auth import SigV4Auth as BotoSigV4
        from botocore.awsrequest import AWSRequest

        headers = {k: v for k, v in request.headers.items() if k.lower() not in ("connection", "accept-encoding")}
        aws = AWSRequest(method=request.method, url=str(request.url), data=request.content, headers=headers)
        BotoSigV4(self._credentials.get_frozen_credentials(), self._service, self._region).add_auth(aws)
        for k, v in aws.headers.items():
            request.headers[k] = v
        yield request


class McpBackend:
    """The MCP tool server over Streamable HTTP. Use as an async context manager."""

    def __init__(self, url: str, token: str = "", timeout_seconds: float = 60, auth: httpx.Auth | None = None) -> None:
        self._url = url
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._http = httpx.AsyncClient(headers=headers, auth=auth, timeout=httpx.Timeout(30, read=timeout_seconds))
        self._client = None
        # AgentCore Gateway prefixes tool names ("platform___get_pods"); the agent only ever sees the plain names.
        self._prefixed: dict[str, str] = {}

    async def __aenter__(self) -> McpBackend:
        from mcp import Client
        from mcp.client.streamable_http import streamable_http_client

        self._client = Client(streamable_http_client(self._url, http_client=self._http))
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.__aexit__(*exc)
        await self._http.aclose()

    async def list_tools(self) -> list[dict[str, Any]]:
        tools = (await self._client.list_tools()).tools
        out = []
        for t in sorted(tools, key=lambda t: t.name):
            name = t.name.split(GATEWAY_SEPARATOR, 1)[-1]
            self._prefixed[name] = t.name
            out.append({"name": name, "description": t.description or "", "input_schema": t.input_schema})
        return out

    async def call(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        result = await self._client.call_tool(self._prefixed.get(name, name), arguments)
        text = "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")
        return ToolResult(text=text, is_error=bool(result.is_error))


# Arguments that identify what a call is about; time windows and limits may differ on replay.
_KEY_ARGUMENTS = {
    "get_pods": ("namespace", "label_selector"), "get_events": ("namespace",),
    "get_pod_logs": ("namespace", "pod", "previous"), "query_prometheus": ("query", "range_minutes"),
    "query_loki": ("query",), "search_runbooks": ("query",), "get_recent_changes": (),
}


class ReplayBackend:
    """Serves recorded tool results from a fixture file (evals/agent-scenarios/fixtures/*.json)."""

    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self._exact: dict[tuple[str, str], dict[str, Any]] = {}
        self._loose: dict[tuple[str, str], dict[str, Any]] = {}
        for r in fixture["responses"]:
            self._exact.setdefault((r["tool"], canonical(r["arguments"])), r)
            self._loose.setdefault((r["tool"], self._key(r["tool"], r["arguments"])), r)

    @classmethod
    def load(cls, path: str | Path) -> ReplayBackend:
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    @staticmethod
    def _key(tool: str, arguments: dict[str, Any]) -> str:
        keys = _KEY_ARGUMENTS.get(tool)
        if keys is None:                 # tools without identifying arguments: any call matches
            return ""
        return canonical({k: arguments.get(k) for k in keys if arguments.get(k) not in (None, "", False)})

    async def list_tools(self) -> list[dict[str, Any]]:
        return self.fixture["tools"]

    async def call(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        r = self._exact.get((name, canonical(arguments))) or self._loose.get((name, self._key(name, arguments)))
        if r is None:
            # Indistinguishable, to the model, from a data source that failed: no hint about the fixture.
            return ToolResult(text="data source error: no data returned for this request", is_error=True,
                              replay_miss=True)
        return ToolResult(text=r["result"], is_error=r.get("is_error", False))
