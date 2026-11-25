import json

import pytest
from pydantic_ai.messages import ModelResponse, RetryPromptPart, TextPart, ThinkingPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.profiles import ModelProfile
from pydantic_ai.usage import RequestUsage

from helpers import FakeBackend, diagnosis
from ops_agent.agent import BUDGET_EXHAUSTED, Agent
from ops_agent.config import AgentConfig

TRIGGER = {"status": "firing", "alerts": [{"labels": {"alertname": "KafkaBrokerDown", "severity": "critical"}}]}
PROFILE = ModelProfile(supports_json_schema_output=True, default_structured_output_mode="native")
USAGE = RequestUsage(input_tokens=1000, output_tokens=200, cache_read_tokens=500)


def scripted(*steps):
    """A model that plays the given steps in order; each step is a list of parts (or a callable)."""
    calls = []

    def respond(messages, info: AgentInfo) -> ModelResponse:
        calls.append((messages, info))
        step = steps[len(calls) - 1]
        parts = step(messages) if callable(step) else step
        return ModelResponse(parts=parts, usage=USAGE, model_name="anthropic.claude-sonnet-4-6")

    return FunctionModel(respond, profile=PROFILE), calls


def answer(**kw) -> list:
    return [TextPart(diagnosis(**kw))]


@pytest.mark.anyio
async def test_investigation_calls_tools_then_returns_a_diagnosis():
    model, calls = scripted(
        [ThinkingPart("check kafka"), ToolCallPart("get_kafka_status", {}, "a"), ToolCallPart("get_nodes", {}, "b")],
        answer(),
    )
    backend = FakeBackend()
    inv = await Agent(AgentConfig(), model, backend).investigate(TRIGGER)

    assert inv.outcome == "diagnosed", inv.error
    assert inv.diagnosis["root_cause_category"] == "kafka_broker_unavailable"
    assert [c.tool for c in inv.tool_calls] == ["get_kafka_status", "get_nodes"]
    assert inv.turns == 2 and inv.usage == {"input": 2000, "output": 400, "cache_read": 1000, "cache_write": 0}
    assert inv.reasoning_summaries == ["check kafka"]
    # Both results went back to the model in one request, matched by id.
    returns = [p for p in calls[1][0][-1].parts if isinstance(p, ToolReturnPart)]
    assert sorted(p.tool_call_id for p in returns) == ["a", "b"]
    info = calls[0][1]
    assert info.model_request_parameters.output_mode == "native"
    assert sorted(t.name for t in info.function_tools) == ["get_kafka_status", "get_nodes", "search_runbooks"]
    assert "KafkaBrokerDown" in calls[0][0][0].parts[-1].content


@pytest.mark.anyio
async def test_tool_budget_turns_further_calls_into_failures():
    config = AgentConfig(max_tool_calls=2)
    model, calls = scripted([ToolCallPart("get_nodes", {}, str(i)) for i in range(3)],
                            answer(evidence_tool="get_nodes"))
    backend = FakeBackend()
    inv = await Agent(config, model, backend).investigate(TRIGGER)
    assert inv.outcome == "diagnosed"
    assert len(backend.calls) == 2
    assert inv.tool_calls[2].is_error and inv.tool_calls[2].result == BUDGET_EXHAUSTED
    failed = [p for p in calls[1][0][-1].parts if isinstance(p, ToolReturnPart) and p.outcome == "failed"]
    assert len(failed) == 1


@pytest.mark.anyio
async def test_request_limit_ends_the_run_as_budget_exhausted():
    config = AgentConfig(max_turns=2)
    model, _ = scripted(*[[ToolCallPart("get_nodes", {}, str(i))] for i in range(5)])
    inv = await Agent(config, model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "budget_exhausted" and inv.diagnosis is None
    assert inv.turns == 2 and inv.usage["input"] == 2000      # usage recorded although the run failed


@pytest.mark.anyio
async def test_refusal_is_recorded_not_parsed():
    def refuse(messages, info):
        return ModelResponse(parts=[], finish_reason="content_filter", usage=USAGE,
                             provider_details={"refusal_category": "cyber"})

    inv = await Agent(AgentConfig(), FunctionModel(refuse, profile=PROFILE), FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "refused" and inv.diagnosis is None


@pytest.mark.anyio
async def test_invalid_output_is_retried_once_then_rejected():
    model, calls = scripted([TextPart('{"summary": "x"}')], [TextPart('{"still": "wrong"}')])
    inv = await Agent(AgentConfig(), model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "invalid_output" and inv.diagnosis is None
    assert len(calls) == 2
    assert any(isinstance(p, RetryPromptPart) for p in calls[1][0][-1].parts)


@pytest.mark.anyio
async def test_an_answer_before_any_tool_call_is_sent_back_once():
    model, calls = scripted(answer(), [ToolCallPart("get_kafka_status", {}, "a")], answer())
    inv = await Agent(AgentConfig(), model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "diagnosed" and [c.tool for c in inv.tool_calls] == ["get_kafka_status"]
    assert any(isinstance(p, RetryPromptPart) for p in calls[1][0][-1].parts)


@pytest.mark.anyio
async def test_evidence_naming_a_tool_with_arguments_is_sent_back():
    model, calls = scripted([ToolCallPart("get_nodes", {}, "a")], answer(evidence_tool="get_nodes (all)"),
                            answer(evidence_tool="get_nodes"))
    inv = await Agent(AgentConfig(), model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "diagnosed" and inv.unsupported_evidence == []
    assert any(isinstance(p, RetryPromptPart) for p in calls[2][0][-1].parts)


@pytest.mark.anyio
async def test_a_diagnosis_without_investigating_is_not_accepted():
    model, _ = scripted(answer(), answer())
    inv = await Agent(AgentConfig(), model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "invalid_output" and inv.diagnosis is None


@pytest.mark.anyio
async def test_unknown_tool_never_reaches_the_backend():
    model, calls = scripted([ToolCallPart("delete_namespace", {"name": "kafka"}, "x")], answer())
    backend = FakeBackend()
    inv = await Agent(AgentConfig(), model, backend).investigate(TRIGGER)
    assert backend.calls == [] and inv.tool_calls == []
    assert any(isinstance(p, RetryPromptPart) for p in calls[1][0][-1].parts)


@pytest.mark.anyio
async def test_evidence_from_tools_never_called_is_flagged():
    model, _ = scripted([ToolCallPart("get_nodes", {}, "a")], answer(evidence_tool="get_kafka_status"))
    inv = await Agent(AgentConfig(), model, FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "diagnosed" and inv.unsupported_evidence == ["get_kafka_status"]


@pytest.mark.anyio
async def test_backend_failures_are_tool_errors_the_model_sees():
    class Broken(FakeBackend):
        async def call(self, name, arguments):
            raise ConnectionError("mcp down")

    model, calls = scripted([ToolCallPart("get_nodes", {}, "a")], answer(evidence_tool="get_nodes"))
    inv = await Agent(AgentConfig(), model, Broken()).investigate(TRIGGER)
    assert inv.tool_calls[0].is_error and "ConnectionError" in inv.tool_calls[0].result
    assert inv.unsupported_evidence == ["get_nodes"]       # an error result is not evidence


@pytest.mark.anyio
async def test_model_errors_become_an_error_outcome():
    def broken(messages, info):
        raise RuntimeError("bedrock down")

    inv = await Agent(AgentConfig(), FunctionModel(broken, profile=PROFILE), FakeBackend()).investigate(TRIGGER)
    assert inv.outcome == "error" and "bedrock down" in inv.error


def test_diagnosis_json_helper_is_valid():
    assert json.loads(diagnosis())["proposed_action"]["action"] == "none"
