"""The agent on Amazon Bedrock AgentCore Runtime: GET /ping; POST /invocations with {"alert": ...} or {"selftest": true}."""
from __future__ import annotations

import logging
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ops_agent.agent import Agent, alert_context, build_model
from ops_agent.backends import McpBackend, SigV4Auth
from ops_agent.config import AgentConfig
from ops_agent.model import bedrock_client
from ops_agent.tracing import Tracer

log = logging.getLogger("ops_agent.runtime")


def build_app(config: AgentConfig) -> Starlette:
    model = build_model(config, bedrock_client(config))
    tracer = Tracer.from_env()

    url = config.gateway_url if config.gateway_url.rstrip("/").endswith("/mcp") else config.gateway_url.rstrip("/") + "/mcp"

    def backend() -> McpBackend:
        return McpBackend(url, auth=SigV4Auth(config.aws_region))

    async def ping(_: Request) -> Response:
        return JSONResponse({"status": "Healthy"})

    async def invocations(request: Request) -> Response:
        body = await request.json()
        if body.get("selftest"):
            started = time.perf_counter()
            async with backend() as tools:
                names = [t["name"] for t in await tools.list_tools()]
                probe = await tools.call("get_kafka_status", {})
            return JSONResponse({"tools": names, "probe_tool": "get_kafka_status", "probe_is_error": probe.is_error,
                                 "probe_result": probe.text[:1500],
                                 "seconds": round(time.perf_counter() - started, 2)})
        if "alert" not in body:
            return JSONResponse({"error": "expected {\"alert\": ...} or {\"selftest\": true}"}, status_code=400)
        async with backend() as tools:
            inv = await Agent(config, model, tools, tracer).investigate(alert_context(body["alert"]))
        if tracer:
            tracer.flush()
        log.info("investigation %s: %s", inv.id, inv.outcome)
        return JSONResponse(inv.to_dict())

    return Starlette(routes=[Route("/ping", ping), Route("/invocations", invocations, methods=["POST"])])
