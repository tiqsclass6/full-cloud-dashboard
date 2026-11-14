import json

import pytest

from ops_agent.agent_app import Gate
from ops_agent.backends import ReplayBackend
from ops_agent.catalog import ACTIONS, ActionError, validate
from ops_agent.config import AgentConfig
from ops_agent.diagnosis import CATEGORIES, output_schema
from ops_agent.slack import esc, investigation_blocks
from ops_agent.store import Proposal


def test_schema_is_strict_everywhere():
    schema = output_schema()

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node["additionalProperties"] is False
                assert set(node["required"]) == set(node["properties"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(schema)
    defs = schema["$defs"]
    assert defs["ProposedAction"]["properties"]["action"]["enum"] == list(ACTIONS)
    assert schema["properties"]["root_cause_category"]["enum"] == list(CATEGORIES)


def test_catalog_rejects_extra_and_missing_parameters():
    with pytest.raises(ActionError, match="needs: pod"):
        validate("restart_pod", {"namespace": "kafka"})
    with pytest.raises(ActionError, match="takes no pod"):
        validate("restart_flink_job", {"pod": "x"})
    with pytest.raises(ActionError, match="unknown action"):
        validate("kubectl_exec", {})
    assert validate("restart_pod", {"namespace": "kafka", "pod": "a-0", "table": ""})[1] == {"namespace": "kafka",
                                                                                              "pod": "a-0"}


def payload(*alerts, group="g1"):
    return {"groupKey": group, "alerts": [{"status": s, "fingerprint": f, "labels": {"severity": sev}}
                                          for s, f, sev in alerts]}


def test_gate_dedupes_filters_and_rate_limits():
    gate = Gate(AgentConfig(max_investigations_per_hour=2, dedupe_minutes=30))
    assert gate.admit(payload(("resolved", "a", "critical"))) == "resolved"
    assert gate.admit(payload(("firing", "a", "info"))) == "severity"
    assert gate.admit(payload(("firing", "a", "critical")), at=1000) == "accepted"
    assert gate.admit(payload(("firing", "a", "critical")), at=1100) == "duplicate"
    assert gate.admit(payload(("firing", "a", "critical"), ("firing", "b", "warning")), at=1200) == "accepted"
    assert gate.admit(payload(("firing", "c", "critical"), group="g2"), at=1300) == "rate_limited"
    assert gate.admit(payload(("firing", "c", "critical"), group="g2"), at=5000) == "accepted"


def test_gate_release_undoes_an_accept_that_could_not_be_queued():
    gate = Gate(AgentConfig(max_investigations_per_hour=1, dedupe_minutes=30))
    alert = payload(("firing", "a", "critical"))
    assert gate.admit(alert, at=1000) == "accepted"
    gate.release(alert, at=1000)                          # the queue was full
    assert gate.admit(alert, at=1060) == "accepted"       # the re-send is investigated, within budget


def test_gate_forgets_fingerprints_after_the_dedupe_window():
    gate = Gate(AgentConfig(max_investigations_per_hour=100, dedupe_minutes=30))
    for i in range(10):
        assert gate.admit(payload(("firing", str(i), "critical"), group=str(i)), at=1000 + i) == "accepted"
    assert len(gate.seen) == 10
    assert gate.admit(payload(("firing", "x", "critical"), group="x"), at=1000 + 31 * 60) == "accepted"
    assert list(gate.seen) == [Gate.fingerprint(payload(("firing", "x", "critical"), group="x"))]


@pytest.mark.anyio
async def test_replay_matches_exact_then_by_key_arguments():
    replay = ReplayBackend({"tools": [], "responses": [
        {"tool": "get_pods", "arguments": {"namespace": "kafka"}, "result": "kafka pods"},
        {"tool": "get_kafka_status", "arguments": {}, "result": "status"},
        {"tool": "get_events", "arguments": {"namespace": "kafka", "minutes": 60}, "result": "events"},
    ]})
    assert (await replay.call("get_pods", {"namespace": "kafka"})).text == "kafka pods"
    assert (await replay.call("get_events", {"namespace": "kafka", "minutes": 15})).text == "events"
    miss = await replay.call("get_pods", {"namespace": "apps"})
    assert miss.is_error and miss.replay_miss and "kafka" not in miss.text
    assert (await replay.call("query_prometheus", {"query": "up"})).replay_miss


def test_slack_text_from_the_model_is_escaped():
    assert esc("<!channel> & <http://evil|click>") == "&lt;!channel&gt; &amp; &lt;http://evil|click&gt;"
    inv = {"id": "i", "outcome": "diagnosed", "trigger": {"alerts": [{"labels": {"alertname": "KafkaBrokerDown"}}]},
           "tool_calls": [], "usage": {"input": 1, "output": 1, "cache_read": 0, "cache_write": 0},
           "diagnosis": {"summary": "<!here> fix now", "root_cause_category": "unknown", "root_cause": "r",
                         "confidence": "low", "impact": "i", "evidence": [{"tool": "t", "finding": "<@U123>"}],
                         "proposed_action": {"action": "restart_pod", "parameters": {}, "rationale": "r"},
                         "runbook": "x"}}
    p = Proposal("p", "i", "restart_pod", {"namespace": "kafka", "pod": "a"}, "h" * 64, "r", "s", 0, 1800, 1)
    blob = json.dumps(investigation_blocks(inv, p, "http://grafana"))
    assert "<!here>" not in blob and "<@U123>" not in blob
    buttons = next(b for b in investigation_blocks(inv, p, "g") if b["type"] == "actions")["elements"]
    assert json.loads(buttons[0]["value"]) == {"id": "p", "hash": "h" * 64}


def test_trace_redaction_catches_prefixed_aws_credentials():
    from ops_agent.redact import redact
    text = "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG AWS_SESSION_TOKEN=IQoJb3JpZ2lu AKIAABCDEFGHIJKLMNOP"
    out = redact(text)
    assert "wJalrXUtnFEMI" not in out and "IQoJb3JpZ2lu" not in out and "AKIAABCDEFGHIJKLMNOP" not in out
