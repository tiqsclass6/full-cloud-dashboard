"""Slack approval messages over Socket Mode (outbound only); model text is escaped before it is posted."""
from __future__ import annotations

import json
import logging
from typing import Any

from ops_agent.approvals import ApprovalService
from ops_agent.store import Proposal

log = logging.getLogger("ops_control.slack")


def esc(text: str, limit: int = 2900) -> str:
    text = (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text if len(text) <= limit else text[:limit] + "..."


def investigation_blocks(inv: dict[str, Any], proposal: Proposal | None, grafana_url: str) -> list[dict[str, Any]]:
    d = inv.get("diagnosis") or {}
    alerts = inv["trigger"].get("alerts") or [{}]
    alertname = (alerts[0].get("labels") or {}).get("alertname", "alert")
    if inv["outcome"] != "diagnosed":
        return [{"type": "section", "text": {"type": "mrkdwn", "text":
                 f":warning: Investigation of *{esc(alertname)}* ended without a diagnosis ({esc(inv['outcome'])}). "
                 f"Investigate by hand; id `{inv['id']}`."}}]
    evidence = "\n".join(f"• `{esc(e['tool'], 60)}` {esc(e['finding'], 300)}" for e in d["evidence"][:6])
    usage = inv["usage"]
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": f"{alertname}: {d['root_cause_category']}"[:150]}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{esc(d['summary'], 600)}*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Confidence*\n{esc(d['confidence'])}"},
            {"type": "mrkdwn", "text": f"*Impact*\n{esc(d['impact'], 900)}"}]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Root cause*\n{esc(d['root_cause'], 1500)}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Evidence*\n{evidence or '(none)'}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"Runbook `{esc(d['runbook'], 120)}` · {len(inv['tool_calls'])} tool calls · "
            f"{usage['input'] + usage['cache_read']:,} in / {usage['output']:,} out tokens · "
            f"<{grafana_url}/d/lakehouse-overview|Grafana> · id `{inv['id']}`"}]},
    ]
    if proposal:
        params = " ".join(f"{k}=`{esc(v, 100)}`" for k, v in proposal.parameters.items()) or "(no parameters)"
        if proposal.pinned:
            params += " · checked before it runs: " + " ".join(
                f"{k}=`{esc(v, 100)[:16]}`" for k, v in proposal.pinned.items())
        blocks += [
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text":
                f"*Proposed action:* `{proposal.action}` {params}\n{esc(proposal.rationale, 800)}\n"
                f"_Needs {proposal.required_approvals} approval(s); expires in "
                f"{round((proposal.expires_at - proposal.created_at) / 60)} min. "
                f"Parameter hash `{proposal.params_hash[:12]}`._"}},
            {"type": "actions", "block_id": "proposal", "elements": [
                {"type": "button", "action_id": "approve", "style": "primary",
                 "text": {"type": "plain_text", "text": "Approve"},
                 "value": json.dumps({"id": proposal.id, "hash": proposal.params_hash}),
                 "confirm": {"title": {"type": "plain_text", "text": "Run this action?"},
                             "text": {"type": "plain_text", "text": f"{proposal.action} will run in the cluster."},
                             "confirm": {"type": "plain_text", "text": "Run it"},
                             "deny": {"type": "plain_text", "text": "Cancel"}}},
                {"type": "button", "action_id": "reject", "style": "danger",
                 "text": {"type": "plain_text", "text": "Reject"},
                 "value": json.dumps({"id": proposal.id, "hash": proposal.params_hash})},
            ]},
        ]
    elif d.get("proposed_action", {}).get("action") in ("none", "escalate"):
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"Recommendation: *{d['proposed_action']['action']}*. {esc(d['proposed_action']['rationale'], 600)}"}]})
    return blocks


class SlackNotifier:
    def __init__(self, bot_token: str, app_token: str, channel: str, grafana_url: str) -> None:
        from slack_bolt import App

        self.app = App(token=bot_token)
        self.app_token = app_token
        self.channel = channel
        self.grafana_url = grafana_url
        self.service: ApprovalService | None = None
        self.app.action("approve")(self._on_click)
        self.app.action("reject")(self._on_click)

    def start(self, service: ApprovalService) -> None:
        from slack_bolt.adapter.socket_mode import SocketModeHandler

        self.service = service
        SocketModeHandler(self.app, self.app_token).connect()      # background thread
        log.info("slack socket mode connected")

    def investigation(self, inv: dict[str, Any], proposal: Proposal | None) -> tuple[str, str] | None:
        blocks = investigation_blocks(inv, proposal, self.grafana_url)
        summary = (inv.get("diagnosis") or {}).get("summary") or inv["outcome"]
        resp = self.app.client.chat_postMessage(channel=self.channel, blocks=blocks, text=esc(summary, 300),
                                                unfurl_links=False)
        return resp["channel"], resp["ts"]

    def decision(self, proposal: Proposal) -> None:
        if not proposal.slack_ts:
            return
        icon = {"approved": ":hourglass:", "executing": ":hourglass:", "succeeded": ":white_check_mark:",
                "failed": ":x:", "rejected": ":no_entry_sign:", "expired": ":clock3:",
                "unknown": ":warning:"}.get(proposal.status, "")
        text = f"{icon} `{proposal.action}` *{proposal.status}*"
        if proposal.decided_by:
            text += f" by {esc(proposal.decided_by, 200)}"
        if proposal.result:
            text += f"\n{esc(proposal.result, 1500)}"
        self.app.client.chat_postMessage(channel=proposal.slack_channel, thread_ts=proposal.slack_ts, text=text)
        if proposal.status != "pending":
            # Remove the buttons as soon as the proposal is no longer pending, so nobody clicks a decided one.
            original = self.app.client.conversations_history(channel=proposal.slack_channel, latest=proposal.slack_ts,
                                                             inclusive=True, limit=1)["messages"]
            if original:
                blocks = [b for b in original[0].get("blocks", []) if b.get("block_id") != "proposal"]
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": text}]})
                self.app.client.chat_update(channel=proposal.slack_channel, ts=proposal.slack_ts, blocks=blocks,
                                            text=text)

    def _on_click(self, ack, body, respond) -> None:
        ack()
        action = body["actions"][0]
        value = json.loads(action["value"])
        user = body["user"]["id"]
        result = self.service.decide(value["id"], user, action["action_id"] == "approve", value["hash"], via="slack")
        respond(replace_original=False, response_type="ephemeral",
                text=f"{'Done' if result.ok else 'Not done'}: {esc(result.message, 500)}")
