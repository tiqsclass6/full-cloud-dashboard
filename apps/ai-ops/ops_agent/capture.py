"""Records an eval fixture from the live cluster while an incident is happening."""
from __future__ import annotations

import time
from typing import Any

from ops_agent.backends import ToolBackend

SNAPSHOT: list[tuple[str, dict[str, Any]]] = [
    ("list_firing_alerts", {}), ("get_kafka_status", {}), ("get_flink_job_status", {}),
    ("get_iceberg_table_health", {}), ("get_argocd_apps", {}), ("get_recent_changes", {"hours": 24}),
    ("get_nodes", {}),
    *[("get_pods", {"namespace": ns}) for ns in ("apps", "kafka", "data")],
    *[("get_events", {"namespace": ns}) for ns in ("apps", "kafka", "data")],
]


async def capture(backend: ToolBackend, alert: dict[str, Any], investigation: dict[str, Any] | None,
                  source: str) -> dict[str, Any]:
    calls = list(SNAPSHOT)
    for name in sorted({(a.get("labels") or {}).get("alertname", "") for a in alert.get("alerts", [])} - {""}):
        calls.append(("search_runbooks", {"query": name}))
    for c in (investigation or {}).get("tool_calls", []):
        calls.append((c["tool"], c["arguments"]))
    responses, seen = [], set()
    for name, args in calls:
        key = (name, repr(sorted(args.items())))
        if key in seen:
            continue
        seen.add(key)
        result = await backend.call(name, args)
        responses.append({"tool": name, "arguments": args, "result": result.text, "is_error": result.is_error})
    return {"captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "source": source,
            "tools": await backend.list_tools(), "responses": responses}
