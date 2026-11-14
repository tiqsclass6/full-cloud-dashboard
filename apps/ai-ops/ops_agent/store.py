"""The audit store for ops-control (Postgres, or in memory for tests); state changes are compare-and-set."""
from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


@dataclass
class Proposal:
    id: str
    investigation_id: str
    action: str
    parameters: dict[str, str]
    params_hash: str
    rationale: str
    summary: str
    created_at: float
    expires_at: float
    required_approvals: int
    # pending -> approved -> executing -> succeeded | failed (or rejected | expired); after a restart approved -> failed, executing -> unknown.
    status: str = "pending"
    approvals: list[str] = field(default_factory=list)
    decided_by: str = ""
    result: str = ""
    slack_channel: str = ""
    slack_ts: str = ""
    # Cluster facts read at proposal time (e.g. the Spark spec's hash); covered by params_hash, re-checked before running.
    pinned: dict[str, str] = field(default_factory=dict)
    updated_at: float = 0.0      # time of the last status change

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Store(Protocol):
    def init(self) -> None: ...
    def save_investigation(self, inv: dict[str, Any]) -> None: ...
    def create_proposal(self, p: Proposal) -> None: ...
    def get_proposal(self, proposal_id: str) -> Proposal | None: ...
    def list_proposals(self, status: str | None = None, limit: int = 20) -> list[Proposal]: ...
    def list_due(self, status: str, field: str, before: float, limit: int = 200) -> list[Proposal]: ...
    def add_approval(self, proposal_id: str, approver: str) -> Proposal | None: ...
    def transition(self, proposal_id: str, from_status: str, to_status: str, **fields: str) -> bool: ...
    def set_slack(self, proposal_id: str, channel: str, ts: str) -> None: ...
    def audit(self, actor: str, event: str, details: dict[str, Any]) -> None: ...


class MemoryStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.investigations: dict[str, dict[str, Any]] = {}
        self.proposals: dict[str, Proposal] = {}
        self.audit_log: list[tuple[str, str, dict[str, Any]]] = []

    def init(self) -> None:
        pass

    def save_investigation(self, inv: dict[str, Any]) -> None:
        self.investigations[inv["id"]] = inv

    def create_proposal(self, p: Proposal) -> None:
        self.proposals[p.id] = p

    def get_proposal(self, proposal_id: str) -> Proposal | None:
        p = self.proposals.get(proposal_id)
        return None if p is None else Proposal(**{**asdict(p), "approvals": list(p.approvals), "pinned": dict(p.pinned)})

    def list_proposals(self, status: str | None = None, limit: int = 20) -> list[Proposal]:
        items = [p for p in self.proposals.values() if status is None or p.status == status]
        return [self.get_proposal(p.id) for p in sorted(items, key=lambda p: -p.created_at)[:limit]]

    def list_due(self, status: str, field: str, before: float, limit: int = 200) -> list[Proposal]:
        assert field in _DUE_FIELDS
        items = [p for p in self.proposals.values() if p.status == status and getattr(p, field) < before]
        return [self.get_proposal(p.id) for p in sorted(items, key=lambda p: getattr(p, field))[:limit]]

    def add_approval(self, proposal_id: str, approver: str) -> Proposal | None:
        with self._lock:
            p = self.proposals.get(proposal_id)
            if p is None or p.status != "pending" or approver in p.approvals:
                return None
            p.approvals.append(approver)
            return self.get_proposal(proposal_id)

    def transition(self, proposal_id: str, from_status: str, to_status: str, **fields: str) -> bool:
        with self._lock:
            p = self.proposals.get(proposal_id)
            if p is None or p.status != from_status:
                return False
            p.status = to_status
            p.updated_at = now()
            for k, v in fields.items():
                setattr(p, k, v)
            return True

    def set_slack(self, proposal_id: str, channel: str, ts: str) -> None:
        p = self.proposals[proposal_id]
        p.slack_channel, p.slack_ts = channel, ts

    def audit(self, actor: str, event: str, details: dict[str, Any]) -> None:
        self.audit_log.append((actor, event, details))


SCHEMA = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.investigations (
  id uuid PRIMARY KEY, started_at timestamptz NOT NULL, finished_at timestamptz,
  outcome text NOT NULL, model text NOT NULL, trigger jsonb NOT NULL, diagnosis jsonb,
  turns int NOT NULL, usage jsonb NOT NULL, unsupported_evidence jsonb NOT NULL, error text NOT NULL,
  record jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS ops.tool_calls (
  investigation_id uuid NOT NULL REFERENCES ops.investigations(id), seq int NOT NULL, tool text NOT NULL,
  arguments jsonb NOT NULL, is_error boolean NOT NULL, duration_ms int NOT NULL, result text NOT NULL,
  PRIMARY KEY (investigation_id, seq));
CREATE TABLE IF NOT EXISTS ops.proposals (
  id uuid PRIMARY KEY, investigation_id uuid NOT NULL REFERENCES ops.investigations(id),
  action text NOT NULL, parameters jsonb NOT NULL, params_hash text NOT NULL, rationale text NOT NULL,
  summary text NOT NULL, created_at timestamptz NOT NULL, expires_at timestamptz NOT NULL,
  required_approvals int NOT NULL, status text NOT NULL, approvals jsonb NOT NULL DEFAULT '[]',
  decided_by text NOT NULL DEFAULT '', result text NOT NULL DEFAULT '',
  slack_channel text NOT NULL DEFAULT '', slack_ts text NOT NULL DEFAULT '',
  pinned jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now());
-- Stores created before these columns existed.
ALTER TABLE ops.proposals ADD COLUMN IF NOT EXISTS pinned jsonb NOT NULL DEFAULT '{}';
ALTER TABLE ops.proposals ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE TABLE IF NOT EXISTS ops.audit_log (
  id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), actor text NOT NULL, event text NOT NULL,
  details jsonb NOT NULL);
"""

_DUE_FIELDS = ("expires_at", "updated_at")

_PROPOSAL_COLUMNS = ("id, investigation_id, action, parameters, params_hash, rationale, summary, "
                     "extract(epoch from created_at), extract(epoch from expires_at), required_approvals, "
                     "status, approvals, decided_by, result, slack_channel, slack_ts, pinned, "
                     "extract(epoch from updated_at)")


class PostgresStore:
    def __init__(self, url: str) -> None:
        self._url = url

    def _conn(self):
        import psycopg

        return psycopg.connect(self._url, autocommit=True, connect_timeout=5)

    def _exec(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def init(self) -> None:
        with self._conn() as conn:
            conn.execute(SCHEMA)

    def save_investigation(self, inv: dict[str, Any]) -> None:
        with self._conn() as conn, conn.transaction():
            conn.execute(
                "INSERT INTO ops.investigations VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s,"
                " %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                (inv["id"], inv["started_at"], inv["finished_at"], inv["outcome"], inv["model"],
                 json.dumps(inv["trigger"]), json.dumps(inv["diagnosis"]), inv["turns"], json.dumps(inv["usage"]),
                 json.dumps(inv["unsupported_evidence"]), inv["error"],
                 json.dumps({k: v for k, v in inv.items() if k != "tool_calls"})))
            for c in inv["tool_calls"]:
                conn.execute("INSERT INTO ops.tool_calls VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                             (inv["id"], c["seq"], c["tool"], json.dumps(c["arguments"]), c["is_error"],
                              c["duration_ms"], c["result"]))

    def create_proposal(self, p: Proposal) -> None:
        self._exec("INSERT INTO ops.proposals (id, investigation_id, action, parameters, params_hash, rationale,"
                   " summary, created_at, expires_at, required_approvals, status, approvals, decided_by, result,"
                   " slack_channel, slack_ts, pinned, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s,"
                   " to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))",
                   (p.id, p.investigation_id, p.action, json.dumps(p.parameters), p.params_hash, p.rationale,
                    p.summary, p.created_at, p.expires_at, p.required_approvals, p.status, json.dumps(p.approvals),
                    p.decided_by, p.result, p.slack_channel, p.slack_ts, json.dumps(p.pinned),
                    p.updated_at or p.created_at))

    @staticmethod
    def _proposal(row: tuple) -> Proposal:
        (pid, inv, action, params, phash, rationale, summary, created, expires, required, status, approvals,
         decided_by, result, channel, ts, pinned, updated) = row
        return Proposal(str(pid), str(inv), action, params, phash, rationale, summary, float(created),
                        float(expires), required, status, list(approvals), decided_by, result, channel, ts,
                        dict(pinned), float(updated))

    def get_proposal(self, proposal_id: str) -> Proposal | None:
        rows = self._exec(f"SELECT {_PROPOSAL_COLUMNS} FROM ops.proposals WHERE id = %s", (proposal_id,))
        return self._proposal(rows[0]) if rows else None

    def list_proposals(self, status: str | None = None, limit: int = 20) -> list[Proposal]:
        where, params = ("WHERE status = %s", (status, limit)) if status else ("", (limit,))
        rows = self._exec(f"SELECT {_PROPOSAL_COLUMNS} FROM ops.proposals {where} ORDER BY created_at DESC LIMIT %s",
                          params)
        return [self._proposal(r) for r in rows]

    def list_due(self, status: str, field: str, before: float, limit: int = 200) -> list[Proposal]:
        assert field in _DUE_FIELDS     # a column name, so it cannot be a query parameter
        rows = self._exec(f"SELECT {_PROPOSAL_COLUMNS} FROM ops.proposals WHERE status = %s"
                          f" AND {field} < to_timestamp(%s) ORDER BY {field} LIMIT %s", (status, before, limit))
        return [self._proposal(r) for r in rows]

    def add_approval(self, proposal_id: str, approver: str) -> Proposal | None:
        rows = self._exec(
            f"UPDATE ops.proposals SET approvals = approvals || to_jsonb(%s::text) WHERE id = %s AND status = 'pending'"
            f" AND NOT approvals ? %s RETURNING {_PROPOSAL_COLUMNS}", (approver, proposal_id, approver))
        return self._proposal(rows[0]) if rows else None

    def transition(self, proposal_id: str, from_status: str, to_status: str, **fields: str) -> bool:
        allowed = {"decided_by", "result"}
        assert set(fields) <= allowed
        sets = "".join(f", {k} = %s" for k in fields)
        rows = self._exec(f"UPDATE ops.proposals SET status = %s, updated_at = now(){sets} WHERE id = %s AND status = %s"
                          " RETURNING id",
                          (to_status, *fields.values(), proposal_id, from_status))
        return bool(rows)

    def set_slack(self, proposal_id: str, channel: str, ts: str) -> None:
        self._exec("UPDATE ops.proposals SET slack_channel = %s, slack_ts = %s WHERE id = %s", (channel, ts, proposal_id))

    def audit(self, actor: str, event: str, details: dict[str, Any]) -> None:
        self._exec("INSERT INTO ops.audit_log (actor, event, details) VALUES (%s, %s, %s)",
                   (actor, event, json.dumps(details, default=str)))


def now() -> float:
    return time.time()
