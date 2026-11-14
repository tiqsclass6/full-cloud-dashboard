import json

import pytest

from helpers import FakeRunner, diagnosis
from ops_agent.approvals import ApprovalService, Signer, TokenError, params_hash
from ops_agent.config import ControlConfig
from ops_agent.store import MemoryStore

KEY = "k" * 40


def investigation(action="restart_pod", **params):
    if action == "restart_pod" and not params:
        params = {"namespace": "kafka", "pod": "lakehouse-dual-role-1"}
    return {"id": "inv-1", "outcome": "diagnosed", "trigger": {"alerts": []},
            "tool_calls": [{"seq": 1, "tool": "get_kafka_status", "arguments": {}, "is_error": False,
                            "duration_ms": 5, "result": "{}"}],
            "usage": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0},
            "diagnosis": json.loads(diagnosis(action=action, **params))}


def service(**overrides):
    config = ControlConfig(signing_key=KEY, approvers=("U1", "U2"), **overrides)
    runner = FakeRunner()
    return ApprovalService(config, MemoryStore(), runner), runner


def proposal_of(svc, result):
    return svc.store.get_proposal(result["proposal_id"])


def test_approved_action_runs_exactly_once():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    assert p.status == "pending" and p.parameters == {"namespace": "kafka", "pod": "lakehouse-dual-role-1"}

    d = svc.decide(p.id, "U1", True, p.params_hash, via="slack")
    assert d.ok and d.status == "succeeded"
    assert runner.runs == [("restart_pod", {"namespace": "kafka", "pod": "lakehouse-dual-role-1"})]
    again = svc.decide(p.id, "U2", True, p.params_hash, via="slack")
    assert not again.ok and runner.runs.__len__() == 1


def test_only_listed_slack_users_can_approve():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    d = svc.decide(p.id, "U9", True, p.params_hash, via="slack")
    assert not d.ok and "approver list" in d.message and runner.runs == []
    assert svc.store.get_proposal(p.id).status == "pending"


def test_approval_is_bound_to_the_parameters_shown():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    other = params_hash("restart_pod", {"namespace": "kafka", "pod": "lakehouse-dual-role-0"})
    d = svc.decide(p.id, "U1", True, other, via="slack")
    assert not d.ok and "changed" in d.message and runner.runs == []


def test_expired_proposal_cannot_be_approved():
    svc, runner = service(approval_ttl_minutes=-1)
    p = proposal_of(svc, svc.submit(investigation()))
    d = svc.decide(p.id, "U1", True, p.params_hash, via="slack")
    assert d.status == "expired" and runner.runs == []


def test_reject():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    assert svc.decide(p.id, "U1", False, p.params_hash, via="slack").status == "rejected"
    assert not svc.decide(p.id, "U2", True, p.params_hash, via="slack").ok and runner.runs == []


def test_two_person_rule():
    svc, runner = service(required_approvals=2)
    p = proposal_of(svc, svc.submit(investigation()))
    assert svc.decide(p.id, "U1", True, p.params_hash, via="slack").status == "pending"
    assert not svc.decide(p.id, "U1", True, p.params_hash, via="slack").ok      # same person twice
    assert runner.runs == []
    assert svc.decide(p.id, "U2", True, p.params_hash, via="slack").status == "succeeded"
    assert len(runner.runs) == 1


def test_cli_approval_is_allowed_without_slack_listing():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    assert svc.decide(p.id, "alice", True, p.params_hash, via="cli").status == "succeeded"
    assert svc.store.get_proposal(p.id).decided_by == "cli:alice"


def test_tokens_are_single_use_and_tamper_proof():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    assert svc.store.add_approval(p.id, "x") and svc.store.transition(p.id, "pending", "approved")
    claims = {"proposal": p.id, "action": p.action, "params_hash": p.params_hash, "exp": 9e12, "nonce": "n"}
    token = svc.signer.sign(claims)
    assert svc.execute(token).status == "succeeded"
    assert not svc.execute(token).ok                         # replay
    forged = Signer("z" * 40).sign(claims)
    assert svc.execute(forged).status == "refused"           # wrong key
    assert len(runner.runs) == 1


def test_parameters_changed_in_the_store_after_approval_are_refused():
    svc, runner = service()
    p = proposal_of(svc, svc.submit(investigation()))
    svc.store.add_approval(p.id, "x")
    svc.store.transition(p.id, "pending", "approved")
    svc.store.proposals[p.id].parameters["pod"] = "postgres-0"       # someone edits the row
    token = svc.signer.sign({"proposal": p.id, "action": p.action, "params_hash": p.params_hash, "exp": 9e12})
    assert svc.execute(token).status == "refused" and runner.runs == []


def test_expired_token():
    signer = Signer(KEY)
    with pytest.raises(TokenError, match="expired"):
        signer.verify(signer.sign({"exp": 1}))


@pytest.mark.parametrize("action,params,reason", [
    ("restart_pod", {"namespace": "kube-system", "pod": "coredns-1"}, "only in"),
    ("restart_pod", {"namespace": "kafka", "pod": "Bad_Name"}, "valid Kubernetes name"),
    ("run_iceberg_compaction", {"table": "lakehouse.other"}, "configured only"),
])
def test_proposals_outside_the_catalogue_are_rejected(action, params, reason):
    svc, runner = service()
    result = svc.submit(investigation(action, **params))
    assert result["proposal_id"] is None and reason in result["rejected_reason"]


@pytest.mark.parametrize("action", ["none", "escalate"])
def test_recommendations_create_no_proposal(action):
    svc, _ = service()
    assert svc.submit(investigation(action))["proposal_id"] is None


def test_dry_run_changes_nothing():
    svc, runner = service(dry_run=True)
    p = proposal_of(svc, svc.submit(investigation("run_iceberg_compaction", table="lakehouse.events")))
    d = svc.decide(p.id, "U1", True, p.params_hash, via="slack")
    assert d.ok and d.message.startswith("dry run") and runner.runs == []


def test_every_step_is_audited():
    svc, _ = service()
    p = proposal_of(svc, svc.submit(investigation()))
    svc.decide(p.id, "U9", True, p.params_hash, via="slack")
    svc.decide(p.id, "U1", True, p.params_hash, via="slack")
    events = [e for _, e, _ in svc.store.audit_log]
    assert events == ["investigation", "proposal_created", "approval_denied", "approval", "approved",
                      "execution_succeeded"]
