"""ops-agent: receives Alertmanager webhooks (filtered, deduplicated, rate-limited), investigates, hands the result to ops-control."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from collections import deque
from typing import Any

import httpx2 as httpx
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ops_agent.agent import Agent, alert_context, build_model
from ops_agent.backends import McpBackend
from ops_agent.config import AgentConfig
from ops_agent.model import bedrock_client
from ops_agent.tracing import Tracer

log = logging.getLogger("ops_agent")

WEBHOOKS = Counter("ops_agent_webhooks_total", "Alertmanager webhooks by decision", ["decision"])
QUEUE = Gauge("ops_agent_queue_length", "Investigations waiting to run")


class Gate:
    """Decides whether an alert group gets an investigation (dedupe + hourly budget)."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.seen: dict[str, float] = {}
        self.started: deque[float] = deque()

    @staticmethod
    def fingerprint(payload: dict[str, Any]) -> str:
        alerts = sorted(a.get("fingerprint", "") for a in payload.get("alerts", []) if a.get("status") == "firing")
        return hashlib.sha256(json.dumps([payload.get("groupKey", ""), alerts]).encode()).hexdigest()[:16]

    def admit(self, payload: dict[str, Any], at: float | None = None) -> str:
        at = at or time.time()
        firing = [a for a in payload.get("alerts", []) if a.get("status") == "firing"]
        if not firing:
            return "resolved"
        if not any((a.get("labels") or {}).get("severity") in self.config.severities for a in firing):
            return "severity"
        key = self.fingerprint(payload)
        window = self.config.dedupe_minutes * 60
        for old in [k for k, t in self.seen.items() if at - t >= window]:
            del self.seen[old]
        if key in self.seen:
            return "duplicate"
        while self.started and at - self.started[0] > 3600:
            self.started.popleft()
        if len(self.started) >= self.config.max_investigations_per_hour:
            return "rate_limited"
        self.seen[key] = at
        self.started.append(at)
        return "accepted"

    def release(self, payload: dict[str, Any], at: float) -> None:
        """Undoes an accept that could not be queued, so Alertmanager's next re-send is investigated."""
        key = self.fingerprint(payload)
        if self.seen.get(key) == at:
            del self.seen[key]
        with contextlib.suppress(ValueError):
            self.started.remove(at)


def invoke_runtime(config: AgentConfig, trigger: dict[str, Any]) -> dict[str, Any]:
    """Runs one investigation on Amazon Bedrock AgentCore Runtime (IRSA: bedrock-agentcore:InvokeAgentRuntime)."""
    import uuid

    import boto3

    client = boto3.client("bedrock-agentcore", region_name=config.aws_region)
    response = client.invoke_agent_runtime(
        agentRuntimeArn=config.agent_runtime_arn, runtimeSessionId=f"investigation-{uuid.uuid4()}",
        contentType="application/json", accept="application/json", payload=json.dumps({"alert": trigger}).encode())
    return json.loads(response["response"].read())


def build_app(config: AgentConfig) -> Starlette:
    gate = Gate(config)
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=20)
    model = build_model(config, bedrock_client(config))
    tracer = Tracer.from_env()

    async def worker() -> None:
        async with httpx.AsyncClient(timeout=30) as http:
            while True:
                trigger = await queue.get()
                QUEUE.set(queue.qsize())
                try:
                    if config.agent_runtime_arn:
                        record = await asyncio.to_thread(invoke_runtime, config, trigger)
                    else:
                        async with McpBackend(config.mcp_url, config.mcp_token) as backend:
                            record = (await Agent(config, model, backend, tracer).investigate(trigger)).to_dict()
                        if tracer:
                            tracer.flush()
                    log.info(json.dumps({"event": "investigation", "id": record["id"], "outcome": record["outcome"],
                                         "tool_calls": len(record["tool_calls"]), "usage": record["usage"],
                                         "where": "agentcore" if config.agent_runtime_arn else "pod"}))
                    resp = await http.post(f"{config.control_url}/v1/investigations", json=record,
                                           headers={"Authorization": f"Bearer {config.control_token}"})
                    resp.raise_for_status()
                except Exception:      # noqa: BLE001 - keep the worker alive
                    log.exception("investigation failed")
                finally:
                    queue.task_done()

    async def alertmanager(request: Request) -> Response:
        payload = await request.json()
        at = time.time()
        decision = gate.admit(payload, at)
        if decision == "accepted":
            try:
                queue.put_nowait(alert_context(payload))
                QUEUE.set(queue.qsize())
            except asyncio.QueueFull:
                gate.release(payload, at)
                decision = "queue_full"
        WEBHOOKS.labels(decision).inc()
        log.info(json.dumps({"event": "webhook", "decision": decision, "group": payload.get("groupKey", "")[:200]}))
        return JSONResponse({"decision": decision})

    async def healthz(_: Request) -> Response:
        return JSONResponse({"status": "ok", "queued": queue.qsize()})

    async def metrics(_: Request) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette):
        task = asyncio.create_task(worker())
        yield
        task.cancel()

    return Starlette(routes=[Route("/v1/alertmanager", alertmanager, methods=["POST"]),
                             Route("/healthz", healthz), Route("/metrics", metrics)],
                     lifespan=lifespan)
