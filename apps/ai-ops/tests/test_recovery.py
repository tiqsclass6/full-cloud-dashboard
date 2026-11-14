"""Approval-flow failure modes: restarts mid-flow, CLI identities, unsupported evidence, pinned specs."""
import json

from helpers import FakeRunner, diagnosis
from ops_agent import approvals
from ops_agent.approvals import ApprovalService
from ops_agent.config import ControlConfig
from ops_agent.slack import SlackNotifier
from ops_agent.store import MemoryStore

KEY = "k" * 40


def investigation(action="restart_pod", called=("get_kafka_status",), evidence_tool="get_kafka_status", **params):
    if action == "restart_pod" and not params:
        params = {"namespace": "kafka", "pod": "lakehouse-dual-role-1"}
    return {"id": "inv-1", "outcome": "diagnosed", "trigger": {"alerts": []},
            "tool_calls": [{"seq": i, "tool": t, "arguments": {}, "is_error": False, "duration_ms": 1, "result": "{}"}
                           for i, t in enumerate(called, 1)],
            "usage": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}, "unsupported_evidence": [],
            "diagnosis": json.loads(diagnosis(action=action, evidence_tool=evidence_tool, **params))}


class RecordingNotifier:
    def __init__(self) -> None:
        self.decisions: list[str] = []

    def investigation(self, inv, proposal):
        return None

    def decision(self, proposal):
        self.decisions.append(proposal.status)


def service(runner=None, **overrides):
    config = ControlConfig(signing_key=KEY, approvers=("U1", "U2"), **overrides)
    runner = runner or FakeRunner()
    notifier = RecordingNotifier()
    return ApprovalService(config, MemoryStore(), runner, notifier), runner, notifier


def proposal_id(svc, inv):
    return svc.submit(inv)["proposal_id"]


def test_two_cli_names_do_not_satisfy_the_two_person_rule():
    svc, runner, _ = service(required_approvals=2)
    pid = proposal_id(svc, investigation())
    h = svc.store.get_proposal(pid).params_hash
    assert svc.decide(pid, "alice", True, h, via="cli").status == "pending"
    second = svc.decide(pid, "bob", True, h, via="cli")
    assert not second.ok and "Slack" in second.message and runner.runs == []
    assert svc.store.get_proposal(pid).approvals == ["cli:alice"]
    assert svc.decide(pid, "U1", True, h, via="slack").status == "succeeded"
    assert len(runner.runs) == 1


def test_a_cli_reject_still_works_after_a_cli_approval():
    svc, runner, _ = service(required_approvals=2)
    pid = proposal_id(svc, investigation())
    h = svc.store.get_proposal(pid).params_hash
    svc.decide(pid, "alice", True, h, via="cli")
    assert svc.decide(pid, "bob", False, h, via="cli").status == "rejected" and runner.runs == []


def test_approved_without_execution_is_closed_as_failed_and_never_runs(monkeypatch):
    svc, runner, notifier = service()
    pid = proposal_id(svc, investigation())
    # ops-control dies right after the move to approved: the in-memory token is gone.
    assert svc.store.transition(pid, "pending", "approved", decided_by="slack:U1")
    assert svc.recover_stale() == 0  # not yet: the token could still be in use

    later = approvals.now() + approvals.TOKEN_TTL_SECONDS + 1
    monkeypatch.setattr(approvals, "now", lambda: later)
    assert svc.recover_stale() == 1
    p = svc.store.get_proposal(pid)
    assert p.status == "failed" and "nothing was changed" in p.result and runner.runs == []
    assert notifier.decisions[-1] == "failed"
    assert not svc.decide(pid, "U2", True, p.params_hash, via="slack").ok and runner.runs == []


def test_execution_interrupted_mid_action_is_closed_as_unknown(monkeypatch):
    svc, runner, notifier = service()
    pid = proposal_id(svc, investigation())
    assert svc.store.transition(pid, "pending", "approved")
    assert svc.store.transition(pid, "approved", "executing")      # ops-control dies inside runner.run

    later = approvals.now() + approvals.EXECUTING_TIMEOUT_SECONDS + 1
    monkeypatch.setattr(approvals, "now", lambda: later)
    assert svc.recover_stale() == 1
    p = svc.store.get_proposal(pid)
    assert p.status == "unknown" and "Check the cluster" in p.result
    assert notifier.decisions[-1] == "unknown" and runner.runs == []


class FakeSlackClient:
    def __init__(self, blocks):
        self.blocks, self.updates, self.posts = blocks, [], []

    def chat_postMessage(self, **kw):
        self.posts.append(kw)

    def conversations_history(self, **kw):
        return {"messages": [{"blocks": self.blocks}]}

    def chat_update(self, **kw):
        self.updates.append(kw)


def test_slack_buttons_disappear_once_a_proposal_is_approved():
    svc, _, _ = service()
    pid = proposal_id(svc, investigation())
    svc.store.set_slack(pid, "C1", "123.4")
    svc.store.transition(pid, "pending", "approved", decided_by="slack:U1")
    notifier = SlackNotifier.__new__(SlackNotifier)       # no Slack connection: only decision() is used
    notifier.app = type("App", (), {})()
    notifier.app.client = FakeSlackClient([{"block_id": "summary"}, {"block_id": "proposal"}])

    notifier.decision(svc.store.get_proposal(pid))

    update = notifier.app.client.updates[0]
    assert [b.get("block_id") for b in update["blocks"] if b.get("block_id")] == ["summary"]


def test_evidence_from_a_tool_that_was_never_called_creates_no_proposal():
    svc, _, _ = service()
    result = svc.submit(investigation(called=("get_nodes",), evidence_tool="get_kafka_status"))
    assert result["proposal_id"] is None and "never called: get_kafka_status" in result["rejected_reason"]


def test_the_agents_own_unsupported_flag_is_honoured_too():
    svc, _, _ = service()
    inv = investigation()
    inv["unsupported_evidence"] = ["query_loki"]
    assert svc.submit(inv)["proposal_id"] is None


def test_compaction_approval_covers_the_spark_spec():
    runner = FakeRunner(spark_spec="spec-v1")
    svc, _, _ = service(runner)
    pid = proposal_id(svc, investigation("run_iceberg_compaction", table="lakehouse.events"))
    p = svc.store.get_proposal(pid)
    assert p.pinned == {"spark_spec_sha256": "spec-v1"}
    assert p.params_hash == approvals.params_hash(p.action, p.parameters, p.pinned)
    assert p.params_hash != approvals.params_hash(p.action, p.parameters)

    runner.spark_spec = "spec-v2"           # Argo CD synced a different schedule before the click
    d = svc.decide(pid, "U1", True, p.params_hash, via="slack")
    assert d.status == "failed" and "changed" in d.message and runner.runs == []


def test_actions_without_cluster_facts_keep_their_original_hash():
    svc, _, _ = service()
    p = svc.store.get_proposal(proposal_id(svc, investigation()))
    assert p.pinned == {} and p.params_hash == approvals.params_hash(p.action, p.parameters)


class FailingNotifier(RecordingNotifier):
    def decision(self, proposal):
        raise RuntimeError("slack: ratelimited")

    def investigation(self, inv, proposal):
        raise RuntimeError("slack: channel_not_found")


def test_a_slack_failure_does_not_stop_an_approved_action():
    config = ControlConfig(signing_key=KEY, approvers=("U1", "U2"))
    runner = FakeRunner()
    svc = ApprovalService(config, MemoryStore(), runner, FailingNotifier())
    pid = proposal_id(svc, investigation())         # posting failed too, but the proposal exists
    assert pid is not None
    d = svc.decide(pid, "U1", True, svc.store.get_proposal(pid).params_hash, via="slack")
    assert d.ok and d.status == "succeeded" and len(runner.runs) == 1


def test_expiry_works_through_a_backlog_oldest_first(monkeypatch):
    svc, _, _ = service()
    ids = []
    for i in range(205):
        inv = investigation()
        inv["id"] = f"inv-{i}"
        ids.append(proposal_id(svc, inv))
        svc.store.proposals[ids[-1]].created_at += i      # distinct ages; the first is the oldest
        svc.store.proposals[ids[-1]].expires_at += i
    monkeypatch.setattr(approvals, "now", lambda: svc.store.proposals[ids[-1]].expires_at + 1)
    assert svc.expire_pending() == 200
    assert svc.store.get_proposal(ids[0]).status == "expired"
    assert svc.store.get_proposal(ids[-1]).status == "pending"
    assert svc.expire_pending() == 5
    assert all(svc.store.get_proposal(i).status == "expired" for i in ids)
