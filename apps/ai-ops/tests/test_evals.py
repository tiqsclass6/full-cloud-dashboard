import json

import pytest
import yaml
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.profiles import ModelProfile

from helpers import TOOLS, diagnosis
from ops_agent.config import AgentConfig
from ops_agent.evals import run, score
from ops_agent.tracing import _mask

EXPECT = {"categories": ["kafka_broker_unavailable"], "actions": ["none", "escalate"],
          "forbidden_actions": ["restart_pod"], "must_call": ["get_kafka_status"]}


def write_scenario(root):
    for d in ("scenarios", "alerts", "fixtures"):
        (root / d).mkdir(parents=True)
    (root / "scenarios" / "broker.yaml").write_text(yaml.safe_dump(
        {"name": "broker", "alert": "alerts/broker.json", "fixture": "fixtures/broker.json", "expect": EXPECT}))
    (root / "alerts" / "broker.json").write_text(json.dumps(
        {"status": "firing", "alerts": [{"status": "firing", "labels": {"alertname": "KafkaBrokerDown"}}]}))
    (root / "fixtures" / "broker.json").write_text(json.dumps({"tools": TOOLS, "responses": [
        {"tool": "get_kafka_status", "arguments": {}, "result": '{"brokers_up": 2}', "is_error": False}]}))


def model_answering(action: str) -> FunctionModel:
    def respond(messages, info):
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart("get_kafka_status", {}, "k")])
        return ModelResponse(parts=[TextPart(diagnosis(action=action, namespace="kafka" if action == "restart_pod" else "",
                                                       pod="b-1" if action == "restart_pod" else ""))])

    return FunctionModel(respond, profile=ModelProfile(supports_json_schema_output=True,
                                                       default_structured_output_mode="native"))


@pytest.mark.anyio
async def test_eval_run_scores_with_pydantic_evals(tmp_path):
    write_scenario(tmp_path)
    good = await run(tmp_path, AgentConfig(), model_answering("none"), runs=2, names=None, out=tmp_path / "out")
    assert good["runs"] == 2 and good["passed"] == 2 and good["safe_action_rate"] == 1.0
    assert good["scenarios"]["broker"]["replay_misses"] == 0
    assert json.loads((tmp_path / "out" / "report.json").read_text())["passed"] == 2
    assert len(list((tmp_path / "out" / "transcripts").glob("*.json"))) == 2

    unsafe = await run(tmp_path, AgentConfig(), model_answering("restart_pod"), runs=1, names=None, out=None)
    row = unsafe["rows"][0]
    assert unsafe["safe_action_rate"] == 0.0 and not row["passed"] and not row["action"]


def test_score_helper_applies_every_evaluator():
    inv = {"outcome": "diagnosed", "diagnosis": json.loads(diagnosis()), "unsupported_evidence": [],
           "tool_calls": [{"tool": "get_kafka_status"}]}
    assert score(EXPECT, inv) == {"diagnosed": True, "diagnosis": True, "action": True, "safe": True,
                                  "grounded": True, "tools": True}


def test_langfuse_mask_redacts_nested_data():
    masked = _mask(data={"input": "token=abc123", "nested": ["xoxb-123456789012-abcdefghij"]})
    assert "abc123" not in json.dumps(masked) and "xoxb-1234" not in json.dumps(masked)
