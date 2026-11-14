"""The AWS path: AgentCore Gateway tool names, SigV4 signing, the runtime contract, forwarding."""
import io
import json
from types import SimpleNamespace as NS

import httpx2
import pytest
from starlette.testclient import TestClient

from ops_agent import agent_app, runtime_app
from ops_agent.backends import McpBackend, SigV4Auth, ToolResult
from ops_agent.config import AgentConfig


@pytest.mark.anyio
async def test_gateway_prefixes_are_hidden_from_the_agent():
    calls = []

    class FakeClient:
        async def list_tools(self):
            return NS(tools=[NS(name="platform___get_pods", description="d", input_schema={}),
                             NS(name="platform___get_nodes", description="d", input_schema={})])

        async def call_tool(self, name, arguments):
            calls.append(name)
            return NS(content=[NS(type="text", text="ok")], is_error=False)

    backend = McpBackend("https://gw/mcp")
    backend._client = FakeClient()
    assert [t["name"] for t in await backend.list_tools()] == ["get_nodes", "get_pods"]
    assert (await backend.call("get_pods", {"namespace": "kafka"})).text == "ok"
    assert calls == ["platform___get_pods"]


def test_sigv4_signs_for_agentcore(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    request = httpx2.Request("POST", "https://gw.gateway.bedrock-agentcore.us-east-1.amazonaws.com/mcp", content=b"{}")
    signed = next(SigV4Auth("us-east-1").auth_flow(request))
    auth = signed.headers["authorization"]
    assert auth.startswith("AWS4-HMAC-SHA256") and "/us-east-1/bedrock-agentcore/aws4_request" in auth
    assert "x-amz-date" in signed.headers


class FakeTools:
    def __init__(self, *_, **__):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def list_tools(self):
        return [{"name": "get_kafka_status", "description": "", "input_schema": {}}]

    async def call(self, name, arguments):
        return ToolResult(text='{"brokers": 3}')


def test_runtime_contract(monkeypatch):
    monkeypatch.setattr(runtime_app, "McpBackend", FakeTools)
    monkeypatch.setattr(runtime_app, "bedrock_client", lambda config: None)
    monkeypatch.setattr(runtime_app, "build_model", lambda config, client: None)
    client = TestClient(runtime_app.build_app(AgentConfig(gateway_url="https://gw/mcp")))
    assert client.get("/ping").json() == {"status": "Healthy"}
    selftest = client.post("/invocations", json={"selftest": True}).json()
    assert selftest["tools"] == ["get_kafka_status"] and selftest["probe_result"] == '{"brokers": 3}'
    assert client.post("/invocations", json={"prompt": "hi"}).status_code == 400


def test_forwarding_to_agentcore(monkeypatch):
    sent = {}

    class FakeAgentCore:
        def invoke_agent_runtime(self, **kwargs):
            sent.update(kwargs)
            return {"response": io.BytesIO(json.dumps({"id": "i1", "outcome": "diagnosed"}).encode())}

    import boto3
    monkeypatch.setattr(boto3, "client", lambda service, region_name: FakeAgentCore())
    config = AgentConfig(agent_runtime_arn="arn:aws:bedrock-agentcore:us-east-1:111122223333:runtime/ops-agent")
    record = agent_app.invoke_runtime(config, {"alerts": [{"labels": {"alertname": "X"}}]})
    assert record == {"id": "i1", "outcome": "diagnosed"}
    assert sent["agentRuntimeArn"] == config.agent_runtime_arn
    assert len(sent["runtimeSessionId"]) >= 33           # AgentCore requires at least 33 characters
    assert json.loads(sent["payload"]) == {"alert": {"alerts": [{"labels": {"alertname": "X"}}]}}
