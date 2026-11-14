"""Test doubles: a recording tool backend and an action runner that changes nothing (the model is
scripted with Pydantic AI's FunctionModel in test_agent.py)."""
from __future__ import annotations

import json
from typing import Any

from ops_agent.backends import ToolResult

TOOLS = [{"name": n, "description": n, "input_schema": {"type": "object", "properties": {}}}
         for n in ("get_kafka_status", "get_nodes", "search_runbooks")]


def diagnosis(category: str = "kafka_broker_unavailable", action: str = "none", evidence_tool: str = "get_kafka_status",
              **params: str) -> str:
    return json.dumps({
        "summary": "Broker 1 is down.", "root_cause_category": category, "root_cause": "node lost",
        "confidence": "high", "impact": "replication degraded, no data loss",
        "evidence": [{"tool": evidence_tool, "finding": "broker 1 not ready"}],
        "proposed_action": {"action": action, "parameters": {"namespace": params.get("namespace", ""),
                                                             "pod": params.get("pod", ""),
                                                             "table": params.get("table", "")},
                            "rationale": "because"},
        "runbook": "docs/runbooks/kafka.md#kafkabrokerdown", "follow_ups": [],
    })


class FakeBackend:
    def __init__(self, results: dict[str, str] | None = None) -> None:
        self.results = results or {"get_kafka_status": '{"brokers": 2}', "get_nodes": '{"nodes": []}'}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def list_tools(self) -> list[dict[str, Any]]:
        return TOOLS

    async def call(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        self.calls.append((name, arguments))
        return ToolResult(self.results.get(name, "{}"))


class FakeRunner:
    """Pins a fixed Spark spec hash for compactions (as KubeRunner pins the live one)."""

    def __init__(self, spark_spec: str = "spec-v1") -> None:
        self.runs: list[tuple[str, dict[str, str]]] = []
        self.pins: list[dict[str, str]] = []
        self.spark_spec = spark_spec

    def pin(self, action: str, parameters: dict[str, str]) -> dict[str, str]:
        return {"spark_spec_sha256": self.spark_spec} if action == "run_iceberg_compaction" else {}

    def run(self, action: str, parameters: dict[str, str], proposal_id: str, pinned: dict[str, str]) -> str:
        if pinned != self.pin(action, parameters):
            raise RuntimeError("the pinned spec changed since approval")
        self.runs.append((action, parameters))
        self.pins.append(pinned)
        return f"ran {action}"
