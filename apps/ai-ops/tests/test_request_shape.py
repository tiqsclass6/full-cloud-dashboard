"""The exact request the agent sends to Claude in Amazon Bedrock, captured at the HTTP layer (no model is called)."""
import json

import httpx2
import pytest
from anthropic import AsyncAnthropicBedrock

from helpers import FakeBackend, diagnosis
from ops_agent.agent import Agent, build_model
from ops_agent.config import AgentConfig

TRIGGER = {"status": "firing", "alerts": [{"labels": {"alertname": "KafkaBrokerDown"}}]}


@pytest.mark.anyio
async def test_bedrock_request_shape():
    requests = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        body = {"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": diagnosis()}], "stop_reason": "end_turn",
                "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 5}}
        return httpx2.Response(200, json=body)

    client = AsyncAnthropicBedrock(aws_region="us-east-1", aws_access_key="test", aws_secret_key="test", max_retries=0,
                                   http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    config = AgentConfig()
    inv = await Agent(config, build_model(config, client), FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "diagnosed", inv.error

    (request,) = requests
    assert request.url.host == "bedrock-runtime.us-east-1.amazonaws.com"
    assert request.url.path == "/model/us.anthropic.claude-sonnet-4-6/invoke"
    assert request.headers["authorization"].startswith("AWS4-HMAC-SHA256")
    body = json.loads(request.content)
    assert body["anthropic_version"] == "bedrock-2023-05-31" and "model" not in body
    assert body["tool_choice"]["type"] == "auto"
    assert body["output_config"]["effort"] == "medium"
    assert body["output_config"]["format"]["type"] == "json_schema"
    assert body["output_config"]["format"]["schema"]["properties"]["proposed_action"]
    assert body["thinking"] == {"type": "adaptive", "display": "summarized"}
    assert "cache_control" in body["tools"][-1]
    assert any("cache_control" in block for block in body["system"])
    assert "temperature" not in body and "budget_tokens" not in json.dumps(body["thinking"])
