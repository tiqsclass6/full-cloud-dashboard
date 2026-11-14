"""PostgresStore against a real Postgres (run-tests.sh starts one and sets OPS_TEST_DATABASE_URL)."""
import os
import time
import uuid

import psycopg
import pytest

from ops_agent.store import PostgresStore, Proposal

URL = os.environ.get("OPS_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="OPS_TEST_DATABASE_URL not set (run-tests.sh sets it)")


@pytest.fixture()
def store():
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS ops CASCADE")
    s = PostgresStore(URL)
    s.init()
    s.init()  # idempotent: a restart runs it again
    return s


def investigation():
    return {"id": str(uuid.uuid4()), "started_at": time.time(), "finished_at": time.time(), "outcome": "diagnosed",
            "model": "test", "trigger": {"alerts": []}, "diagnosis": {"summary": "s"}, "turns": 1,
            "usage": {"input": 1}, "unsupported_evidence": [], "error": "",
            "tool_calls": [{"seq": 1, "tool": "get_pods", "arguments": {"namespace": "apps"}, "is_error": False,
                            "duration_ms": 3, "result": "{}"}]}


def proposal(inv_id, **kw):
    t = time.time()
    return Proposal(id=str(uuid.uuid4()), investigation_id=inv_id, action="run_iceberg_compaction",
                    parameters={"table": "lakehouse.events"}, params_hash="h" * 64, rationale="r", summary="s",
                    created_at=t, expires_at=t + 900, required_approvals=2,
                    pinned={"spark_spec_sha256": "abc"}, updated_at=t, **kw)


def test_round_trip_including_pinned_facts(store):
    inv = investigation()
    store.save_investigation(inv)
    store.save_investigation(inv)  # a retried submit does not fail or duplicate
    p = proposal(inv["id"])
    store.create_proposal(p)
    got = store.get_proposal(p.id)
    assert (got.action, got.parameters, got.pinned, got.status, got.approvals) == \
        (p.action, p.parameters, {"spark_spec_sha256": "abc"}, "pending", [])
    assert abs(got.created_at - p.created_at) < 0.01 and abs(got.updated_at - p.updated_at) < 0.01
    store.set_slack(p.id, "C1", "123.4")
    assert (store.get_proposal(p.id).slack_channel, store.get_proposal(p.id).slack_ts) == ("C1", "123.4")


def test_approvals_are_a_jsonb_set_and_only_while_pending(store):
    inv = investigation()
    store.save_investigation(inv)
    p = proposal(inv["id"])
    store.create_proposal(p)
    assert store.add_approval(p.id, "slack:U1").approvals == ["slack:U1"]
    assert store.add_approval(p.id, "slack:U1") is None                    # one person counts once
    assert store.add_approval(p.id, "cli:alice").approvals == ["slack:U1", "cli:alice"]
    assert store.transition(p.id, "pending", "rejected", decided_by="slack:U1")
    assert store.add_approval(p.id, "slack:U2") is None                    # no longer pending


def test_transitions_are_compare_and_set_and_stamp_the_time(store):
    inv = investigation()
    store.save_investigation(inv)
    p = proposal(inv["id"])
    store.create_proposal(p)
    before = store.get_proposal(p.id).updated_at
    time.sleep(0.05)
    assert store.transition(p.id, "pending", "approved", decided_by="slack:U1,slack:U2")
    assert not store.transition(p.id, "pending", "approved")               # the second racer loses
    assert store.transition(p.id, "approved", "executing")
    assert not store.transition(p.id, "approved", "executing")             # single execution
    assert store.transition(p.id, "executing", "succeeded", result="ok")
    got = store.get_proposal(p.id)
    assert (got.status, got.result, got.decided_by) == ("succeeded", "ok", "slack:U1,slack:U2")
    assert got.updated_at > before
    assert [x.id for x in store.list_proposals("succeeded")] == [p.id]
    assert store.list_proposals("pending") == []


def test_init_upgrades_a_store_created_before_pinned_and_updated_at(store):
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute("ALTER TABLE ops.proposals DROP COLUMN pinned, DROP COLUMN updated_at")
    store.init()
    inv = investigation()
    store.save_investigation(inv)
    p = proposal(inv["id"])
    store.create_proposal(p)
    assert store.get_proposal(p.id).pinned == {"spark_spec_sha256": "abc"}


def test_audit_rows_are_written(store):
    store.audit("ops-control", "proposal_created", {"proposal": "x", "at": time.time()})
    with psycopg.connect(URL) as conn:
        assert conn.execute("SELECT actor, event FROM ops.audit_log").fetchall() == [("ops-control", "proposal_created")]


def test_list_due_filters_and_orders_in_the_query(store):
    inv = investigation()
    store.save_investigation(inv)
    t = time.time()
    ps = [proposal(inv["id"]) for _ in range(3)]
    for i, p in enumerate(ps):
        p.expires_at = t - 100 + i * 60          # expired, expired, not yet
        store.create_proposal(p)
    assert [p.id for p in store.list_due("pending", "expires_at", t)] == [ps[0].id, ps[1].id]
    assert [p.id for p in store.list_due("pending", "expires_at", t, limit=1)] == [ps[0].id]
    assert store.list_due("approved", "updated_at", t + 1) == []
