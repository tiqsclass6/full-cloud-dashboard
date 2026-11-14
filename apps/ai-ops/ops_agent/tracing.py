"""Langfuse tracing, one redacted trace per investigation; enabled when the LANGFUSE_* variables are set."""
from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from typing import Any

from ops_agent.redact import redact


def _mask(*, data: Any, **_: Any) -> Any:
    """Redact every string, however deeply nested (Langfuse calls this on inputs, outputs and metadata)."""
    if isinstance(data, str):
        return redact(data)
    if isinstance(data, dict):
        return {k: _mask(data=v) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        return [_mask(data=v) for v in data]
    return data


class _Observation:
    def __init__(self, observation: Any) -> None:
        self._observation = observation

    def finish(self, inv: Any) -> None:
        self._observation.update(
            output=inv.diagnosis or {"outcome": inv.outcome, "error": inv.error},
            level="DEFAULT" if inv.outcome == "diagnosed" else "ERROR",
            status_message=inv.outcome,
            metadata={"investigation_id": inv.id, "outcome": inv.outcome, "tool_calls": len(inv.tool_calls),
                      "turns": inv.turns, "usage": inv.usage, "models": inv.models_used,
                      "unsupported_evidence": inv.unsupported_evidence},
        )


class Tracer:
    def __init__(self, environment: str = "local", release: str = "") -> None:
        from langfuse import Langfuse

        self.client = Langfuse(environment=environment, release=release or None, mask=_mask)

    @classmethod
    def from_env(cls) -> Tracer | None:
        if not all(os.environ.get(k) for k in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL")):
            return None
        return cls(environment=os.environ.get("LANGFUSE_TRACING_ENVIRONMENT", "local"))

    @contextlib.contextmanager
    def investigation(self, inv: Any) -> Iterator[_Observation]:
        alerts = [a.get("labels", {}).get("alertname", "") for a in inv.trigger.get("alerts", [])]
        with self.client.start_as_current_observation(
            name=f"investigate {', '.join(sorted(set(alerts)))[:120]}", as_type="agent", input=inv.trigger,
            metadata={"investigation_id": inv.id, "model": inv.model},
        ) as observation:
            inv.trace_id = self.client.get_current_trace_id() or ""
            yield _Observation(observation)

    def score(self, trace_id: str, name: str, value: float | bool, comment: str = "") -> None:
        if trace_id:
            self.client.create_score(trace_id=trace_id, name=name, value=float(value), comment=comment or None,
                                     data_type="BOOLEAN" if isinstance(value, bool) else "NUMERIC")

    def flush(self) -> None:
        self.client.flush()
