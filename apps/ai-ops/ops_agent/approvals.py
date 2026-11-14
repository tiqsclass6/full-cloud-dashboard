"""Human approval for every proposed change: re-validated, hash-bound, single-use signed token, recovery after restarts."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from prometheus_client import Counter

from ops_agent.catalog import ActionError, validate
from ops_agent.config import ControlConfig
from ops_agent.store import Proposal, Store, now

log = logging.getLogger("ops_control")

PROPOSALS = Counter("ops_control_proposals_total", "Proposals by action and final decision", ["action", "decision"])
EXECUTIONS = Counter("ops_control_actions_total", "Executed actions by action and result", ["action", "result"])

TOKEN_TTL_SECONDS = 60
EXECUTING_TIMEOUT_SECONDS = 600     # longer than any action takes (the slowest is a Kubernetes API call)


def params_hash(action: str, parameters: dict[str, str], pinned: dict[str, str] | None = None) -> str:
    content: dict[str, Any] = {"action": action, "parameters": parameters}
    if pinned:
        content["pinned"] = pinned
    body = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


class TokenError(Exception):
    pass


class Signer:
    """HMAC-SHA256 execution tokens: `v1.<claims>.<signature>`, both base64url."""

    def __init__(self, key: str) -> None:
        if len(key) < 32:
            raise ValueError("ACTION_SIGNING_KEY must be at least 32 characters")
        self._key = key.encode()

    def sign(self, claims: dict[str, Any]) -> str:
        body = base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True).encode()).decode().rstrip("=")
        sig = hmac.new(self._key, f"v1.{body}".encode(), hashlib.sha256).digest()
        return f"v1.{body}.{base64.urlsafe_b64encode(sig).decode().rstrip('=')}"

    def verify(self, token: str) -> dict[str, Any]:
        try:
            version, body, sig = token.split(".")
        except ValueError as e:
            raise TokenError("malformed token") from e
        expected = hmac.new(self._key, f"{version}.{body}".encode(), hashlib.sha256).digest()
        if version != "v1" or not hmac.compare_digest(base64.urlsafe_b64encode(expected).decode().rstrip("="), sig):
            raise TokenError("bad signature")
        claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        if claims["exp"] < now():
            raise TokenError("token expired")
        return claims


class Notifier(Protocol):
    def investigation(self, inv: dict[str, Any], proposal: Proposal | None) -> tuple[str, str] | None: ...
    def decision(self, proposal: Proposal) -> None: ...


class LogNotifier:
    """Used when Slack is not configured: the investigation is logged; approvals go through the CLI."""

    def investigation(self, inv: dict[str, Any], proposal: Proposal | None) -> tuple[str, str] | None:
        d = inv.get("diagnosis") or {}
        log.info(json.dumps({"event": "investigation", "id": inv["id"], "outcome": inv["outcome"],
                             "summary": d.get("summary"), "proposal": proposal.id if proposal else None}))
        return None

    def decision(self, proposal: Proposal) -> None:
        log.info(json.dumps({"event": "decision", "proposal": proposal.id, "status": proposal.status}))


class ActionRunner(Protocol):
    def pin(self, action: str, parameters: dict[str, str]) -> dict[str, str]: ...
    def run(self, action: str, parameters: dict[str, str], proposal_id: str, pinned: dict[str, str]) -> str: ...


@dataclass
class Decision:
    ok: bool
    status: str
    message: str


class ApprovalService:
    def __init__(self, config: ControlConfig, store: Store, runner: ActionRunner, notifier: Notifier | None = None):
        self.config = config
        self.store = store
        self.runner = runner
        self.notifier = notifier or LogNotifier()
        self.signer = Signer(config.signing_key)

    def submit(self, inv: dict[str, Any]) -> dict[str, Any]:
        self.store.save_investigation(inv)
        self.store.audit("ops-agent", "investigation", {"id": inv["id"], "outcome": inv["outcome"]})
        proposal, rejected = None, ""
        diagnosis = inv.get("diagnosis") or {}
        proposed = diagnosis.get("proposed_action") or {}
        if inv.get("outcome") == "diagnosed" and proposed.get("action"):
            try:
                spec, params = validate(proposed["action"], proposed.get("parameters") or {})
                if spec.executable:
                    _check_evidence(inv, diagnosis)
                    proposal = self._create(inv["id"], spec.name, params, proposed.get("rationale", ""),
                                            diagnosis.get("summary", ""))
            except ActionError as e:
                rejected = str(e)
                self.store.audit("ops-control", "proposal_rejected", {"investigation": inv["id"], "reason": rejected,
                                                                      "proposed": proposed})
        try:
            posted = self.notifier.investigation(inv, proposal)
        except Exception:      # noqa: BLE001
            log.exception("could not post investigation %s", inv["id"])
            posted = None
        if proposal and posted:
            self.store.set_slack(proposal.id, *posted)
        return {"investigation_id": inv["id"], "proposal_id": proposal.id if proposal else None,
                "rejected_reason": rejected}

    def _create(self, inv_id: str, action: str, params: dict[str, str], rationale: str, summary: str) -> Proposal:
        try:
            pinned = self.runner.pin(action, params)
        except Exception as e:      # noqa: BLE001 - e.g. the Spark schedule is missing: no proposal
            raise ActionError(f"cannot prepare {action}: {type(e).__name__}: {e}"[:500]) from e
        t = now()
        p = Proposal(id=str(uuid.uuid4()), investigation_id=inv_id, action=action, parameters=params,
                     params_hash=params_hash(action, params, pinned), rationale=rationale[:2000],
                     summary=summary[:1000], created_at=t, expires_at=t + self.config.approval_ttl_minutes * 60,
                     required_approvals=self.config.required_approvals, pinned=pinned, updated_at=t)
        self.store.create_proposal(p)
        self.store.audit("ops-control", "proposal_created", {"proposal": p.id, "action": action, "parameters": params,
                                                             "pinned": pinned})
        return p

    def decide(self, proposal_id: str, approver: str, approve: bool, seen_hash: str, via: str) -> Decision:
        p = self.store.get_proposal(proposal_id)
        if p is None:
            return Decision(False, "unknown", "no such proposal")
        if via == "slack" and approver not in self.config.approvers:
            self.store.audit(approver, "approval_denied", {"proposal": p.id, "reason": "not an approver"})
            return Decision(False, p.status, "you are not on the approver list")
        if not hmac.compare_digest(seen_hash, p.params_hash):
            self.store.audit(approver, "approval_denied", {"proposal": p.id, "reason": "parameter hash mismatch"})
            return Decision(False, p.status, "the proposal changed since it was shown; nothing was approved")
        if p.status != "pending":
            return Decision(False, p.status, f"proposal is already {p.status}")
        if p.expires_at < now():
            self._expire(p)
            return Decision(False, "expired", "proposal expired; nothing was run")
        who = f"{via}:{approver}"
        if approve and via == "cli" and any(a.startswith("cli:") for a in p.approvals):
            # Anyone who can exec into ops-control can type any name, so CLI approvals are one identity.
            self.store.audit(who, "approval_denied", {"proposal": p.id, "reason": "second CLI approval"})
            return Decision(False, p.status, "a CLI approval is already counted; the next one must come from Slack")
        if not approve:
            if self.store.transition(p.id, "pending", "rejected", decided_by=who):
                self._decided(p.id, who, "rejected")
                return Decision(True, "rejected", f"rejected by {who}")
            return Decision(False, p.status, "proposal changed state; try again")

        updated = self.store.add_approval(p.id, who)
        if updated is None:
            return Decision(False, p.status, "already approved by you, or no longer pending")
        self.store.audit(who, "approval", {"proposal": p.id, "approvals": updated.approvals})
        if len(updated.approvals) < updated.required_approvals:
            return Decision(True, "pending", f"{len(updated.approvals)} of {updated.required_approvals} approvals")
        if not self.store.transition(p.id, "pending", "approved", decided_by=",".join(updated.approvals)):
            return Decision(False, p.status, "proposal changed state; nothing was run")
        self._decided(p.id, who, "approved")
        token = self.signer.sign({"proposal": p.id, "action": p.action, "params_hash": p.params_hash,
                                  "exp": now() + TOKEN_TTL_SECONDS, "nonce": uuid.uuid4().hex})
        result = self.execute(token)
        return Decision(result.ok, result.status, result.message)

    def _decided(self, proposal_id: str, who: str, decision: str) -> None:
        p = self.store.get_proposal(proposal_id)
        PROPOSALS.labels(p.action, decision).inc()
        self.store.audit(who, decision, {"proposal": proposal_id})
        self._notify(p)

    def _notify(self, p: Proposal) -> None:
        try:
            self.notifier.decision(p)
        except Exception:      # noqa: BLE001
            log.exception("could not notify the decision on proposal %s (%s)", p.id, p.status)

    def _expire(self, p: Proposal) -> None:
        if self.store.transition(p.id, "pending", "expired"):
            self._decided(p.id, "ops-control", "expired")

    def expire_pending(self) -> int:
        expired = self.store.list_due("pending", "expires_at", now())
        for p in expired:
            self._expire(p)
        return len(expired)

    def recover_stale(self) -> int:
        """Closes proposals a restart left in `approved` (failed: nothing ran) or `executing` (unknown)."""
        closed = 0
        for p in self.store.list_due("approved", "updated_at", now() - TOKEN_TTL_SECONDS):
            if self.store.transition(
                    p.id, "approved", "failed", result="not run: ops-control stopped between approval and execution;"
                                                       " nothing was changed. Propose the action again if needed."):
                self._decided(p.id, "ops-control", "failed")
                closed += 1
        for p in self.store.list_due("executing", "updated_at", now() - EXECUTING_TIMEOUT_SECONDS):
            if self.store.transition(
                    p.id, "executing", "unknown", result="outcome unknown: ops-control stopped while the action ran."
                                                         " Check the cluster before acting again."):
                EXECUTIONS.labels(p.action, "unknown").inc()
                self._decided(p.id, "ops-control", "unknown")
                closed += 1
        return closed

    def execute(self, token: str) -> Decision:
        try:
            claims = self.signer.verify(token)
        except TokenError as e:
            self.store.audit("ops-control", "execution_refused", {"reason": str(e)})
            return Decision(False, "refused", f"execution refused: {e}")
        p = self.store.get_proposal(claims["proposal"])
        if (p is None or claims["action"] != p.action or claims["params_hash"] != p.params_hash
                or params_hash(p.action, p.parameters, p.pinned) != p.params_hash):
            self.store.audit("ops-control", "execution_refused", {"proposal": claims["proposal"],
                                                                  "reason": "token does not match the proposal"})
            return Decision(False, "refused", "execution refused: token does not match the stored proposal")
        if not self.store.transition(p.id, "approved", "executing"):
            return Decision(False, p.status, f"execution refused: proposal is {p.status} (tokens are single-use)")
        try:
            spec, params = validate(p.action, p.parameters)
            if self.config.dry_run:
                result = f"dry run: would run {spec.name} {params}"
            else:
                result = self.runner.run(spec.name, params, p.id, p.pinned)
            status = "succeeded"
        except Exception as e:      # noqa: BLE001 - recorded as the action's result
            result, status = f"{type(e).__name__}: {e}"[:1000], "failed"
        self.store.transition(p.id, "executing", status, result=result)
        EXECUTIONS.labels(p.action, status).inc()
        self.store.audit("ops-control", f"execution_{status}", {"proposal": p.id, "action": p.action,
                                                               "parameters": p.parameters, "result": result})
        self._notify(self.store.get_proposal(p.id))
        return Decision(status == "succeeded", status, result)


def _check_evidence(inv: dict[str, Any], diagnosis: dict[str, Any]) -> None:
    """An executable proposal needs evidence, all of it from tools the agent actually called."""
    if not diagnosis.get("evidence"):
        raise ActionError("the diagnosis cites no evidence")
    # Recomputed here from the recorded tool calls, not only taken from the agent's own flag.
    called = {c.get("tool") for c in inv.get("tool_calls") or []}
    cited = {e.get("tool") for e in diagnosis["evidence"]}
    unsupported = sorted(set(inv.get("unsupported_evidence") or []) | (cited - called))
    if unsupported:
        raise ActionError(f"the diagnosis cites tools that were never called: {', '.join(unsupported)}")
