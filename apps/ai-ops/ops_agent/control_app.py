"""ops-control: the audit store, the approval flow (Slack or CLI) and the action executor."""
from __future__ import annotations

import asyncio
import contextlib
import hmac
import logging

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ops_agent.approvals import ApprovalService
from ops_agent.config import ControlConfig

log = logging.getLogger("ops_control")


def build_app(config: ControlConfig, service: ApprovalService) -> Starlette:
    def authorized(request: Request) -> bool:
        header = request.headers.get("authorization", "")
        return bool(config.control_token) and hmac.compare_digest(header, f"Bearer {config.control_token}")

    async def investigations(request: Request) -> Response:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        result = await run_in_threadpool(service.submit, await request.json())
        return JSONResponse(result)

    async def proposals(request: Request) -> Response:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        items = await run_in_threadpool(service.store.list_proposals, request.query_params.get("status"))
        return JSONResponse([p.to_dict() for p in items])

    async def healthz(_: Request) -> Response:
        return JSONResponse({"status": "ok", "dry_run": config.dry_run})

    async def metrics(_: Request) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    async def expiry_loop() -> None:
        while True:
            try:
                await run_in_threadpool(service.expire_pending)
                await run_in_threadpool(service.recover_stale)
            except Exception:      # noqa: BLE001
                log.exception("expiry check failed")
            await asyncio.sleep(60)

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette):
        task = asyncio.create_task(expiry_loop())
        yield
        task.cancel()

    return Starlette(routes=[Route("/v1/investigations", investigations, methods=["POST"]),
                             Route("/v1/proposals", proposals), Route("/healthz", healthz),
                             Route("/metrics", metrics)], lifespan=lifespan)
