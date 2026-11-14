"""The remediation catalogue: the only actions the agent may propose, with strict parameter rules (checked again by ops-control)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Kubernetes object names (DNS-1123 subdomain, as used for pod names).
_NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")


class ActionError(ValueError):
    """The proposal is not allowed; the message says why."""


@dataclass(frozen=True)
class Action:
    name: str
    description: str
    parameters: tuple[str, ...]     # parameter names that must be set (others must be empty)
    executable: bool = True         # False: a recommendation for a human, nothing runs

    def validate(self, params: dict[str, str]) -> dict[str, str]:
        clean = {k: (params.get(k) or "").strip() for k in PARAMETER_NAMES}
        missing = [k for k in self.parameters if not clean[k]]
        extra = [k for k in PARAMETER_NAMES if clean[k] and k not in self.parameters]
        if missing:
            raise ActionError(f"{self.name} needs: {', '.join(missing)}")
        if extra:
            raise ActionError(f"{self.name} takes no {', '.join(extra)}")
        _RULES[self.name](clean)
        return {k: clean[k] for k in self.parameters}


def _restart_pod(p: dict[str, str]) -> None:
    if p["namespace"] not in RESTARTABLE_NAMESPACES:
        raise ActionError(f"pods can be restarted only in {', '.join(RESTARTABLE_NAMESPACES)}")
    if not _NAME.match(p["pod"]):
        raise ActionError("pod is not a valid Kubernetes name")


def _compaction(p: dict[str, str]) -> None:
    if p["table"] not in COMPACTION_SCHEDULES:
        raise ActionError(f"compaction is configured only for {', '.join(COMPACTION_SCHEDULES)}")


RESTARTABLE_NAMESPACES = ("apps", "kafka", "data")
# Iceberg table -> the ScheduledSparkApplication whose template the one-off run copies.
COMPACTION_SCHEDULES = {"lakehouse.events": "events-compaction"}
PARAMETER_NAMES = ("namespace", "pod", "table")

ACTIONS: dict[str, Action] = {a.name: a for a in (
    Action("none", "No action: the system recovered or is recovering on its own.", (), executable=False),
    Action("escalate", "Page a human: the fix needs judgement, a code or config change, or access the agent "
                       "does not have.", (), executable=False),
    Action("restart_pod", "Delete one pod so its controller recreates it (StatefulSet, Deployment, operator). "
                          "Only for a pod that is stuck or wedged, never for a pod that is merely unreachable.",
           ("namespace", "pod")),
    Action("restart_flink_job", "Restart the Flink streaming job from its latest checkpoint (FlinkDeployment "
                                "restartNonce). For a job that is FAILED or stuck restarting.", ()),
    Action("run_iceberg_compaction", "Run the Iceberg compaction Spark job once now (the same job as the hourly "
                                     "schedule). For small-file build-up or overdue compaction.", ("table",)),
)}

_RULES = {
    "none": lambda p: None,
    "escalate": lambda p: None,
    "restart_pod": _restart_pod,
    "restart_flink_job": lambda p: None,
    "run_iceberg_compaction": _compaction,
}


def validate(action: str, params: dict[str, Any]) -> tuple[Action, dict[str, str]]:
    spec = ACTIONS.get(action)
    if spec is None:
        raise ActionError(f"unknown action '{action}'; allowed: {', '.join(ACTIONS)}")
    if not isinstance(params, dict) or not all(isinstance(v, str) for v in params.values()):
        raise ActionError("parameters must be strings")
    return spec, spec.validate(params)


def describe() -> str:
    """The catalogue as prompt text."""
    lines = []
    for a in ACTIONS.values():
        params = ", ".join(a.parameters) or "no parameters"
        lines.append(f"- {a.name} ({params}): {a.description}")
    return "\n".join(lines)
