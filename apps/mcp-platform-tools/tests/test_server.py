import json
import os
from pathlib import Path

import pytest
from mcp import Client
from starlette.testclient import TestClient

from fakes import FakeSources
from platform_tools.config import Config
from platform_tools.server import BearerToken, build_server
from platform_tools.tools import READ_ONLY_TOOLS

RUNBOOKS = Path(os.environ.get("RUNBOOK_DIR") or Path(__file__).resolve().parents[3] / "docs" / "runbooks")
WRITE_VERBS = ("delete", "restart", "scale", "patch", "create", "update", "apply", "exec", "trigger", "run_")


@pytest.fixture
def server():
    return build_server(Config(runbook_dir=str(RUNBOOKS), max_result_chars=2000), FakeSources())


@pytest.mark.anyio
async def test_exposes_exactly_the_read_only_tools(server):
    async with Client(server) as client:
        tools = (await client.list_tools()).tools
    names = {t.name for t in tools}
    assert names == set(READ_ONLY_TOOLS)
    assert not [n for n in names if any(v in n for v in WRITE_VERBS)]
    pods = next(t for t in tools if t.name == "get_pods")
    assert pods.input_schema["required"] == ["namespace"]
    assert "untrusted" in next(t for t in tools if t.name == "get_pod_logs").description


@pytest.mark.anyio
async def test_call_returns_json_text(server):
    async with Client(server) as client:
        result = await client.call_tool("get_kafka_status", {})
    assert not result.is_error
    body = json.loads(result.content[0].text)
    assert body["metrics"]["under_replicated_partitions"] == 12


@pytest.mark.anyio
async def test_bad_input_is_a_tool_error_the_model_can_read(server):
    async with Client(server) as client:
        result = await client.call_tool("get_pods", {"namespace": "kube-system"})
    assert result.is_error
    assert "not readable" in result.content[0].text


@pytest.mark.anyio
async def test_answers_are_capped():
    small = build_server(Config(runbook_dir=str(RUNBOOKS), max_result_chars=300), FakeSources())
    async with Client(small) as client:
        result = await client.call_tool("query_prometheus", {"query": "up", "range_minutes": 600})
    text = result.content[0].text
    assert "answer cut at 300 of" in text and len(text) < 400


def test_bearer_token_guards_only_the_mcp_path(server):
    app = BearerToken(server.streamable_http_app(stateless_http=True, json_response=True), "s3cret")
    with TestClient(app) as http:
        assert http.get("/healthz").status_code == 200
        assert http.post("/mcp", json={}).status_code == 401
        assert http.post("/mcp", json={}, headers={"Authorization": "Bearer wrong"}).status_code == 401
        ok = http.post("/mcp", headers={"Authorization": "Bearer s3cret", "Accept": "application/json, text/event-stream"},
                       json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
        assert ok.status_code != 401


def test_tls_listener_shares_the_app_without_a_second_lifespan(tmp_path, monkeypatch, server):
    from platform_tools.server import listeners

    app = server.streamable_http_app(stateless_http=True, json_response=True)
    assert [c.port for c in listeners(app)] == [8080]          # no certificate: plain HTTP only
    (tmp_path / "tls.crt").write_text("x")
    (tmp_path / "tls.key").write_text("x")
    monkeypatch.setenv("TLS_CERT_FILE", str(tmp_path / "tls.crt"))
    monkeypatch.setenv("TLS_KEY_FILE", str(tmp_path / "tls.key"))
    plain, tls = listeners(app)
    assert (plain.port, tls.port) == (8080, 8443)
    assert plain.lifespan == "auto" and tls.lifespan == "off"  # the session manager starts once
    assert tls.ssl_certfile == str(tmp_path / "tls.crt")
