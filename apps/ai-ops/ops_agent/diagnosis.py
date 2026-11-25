"""The agent's final answer: a structured diagnosis with closed sets of categories and actions."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, create_model

from ops_agent.catalog import ACTIONS, PARAMETER_NAMES

CATEGORIES = (
    "kafka_broker_unavailable",     # a broker is down or unreachable
    "kafka_replication_degraded",   # brokers up, but partitions under-replicated / offline / no controller
    "flink_job_failure",            # the streaming job failed, is restarting, or is not running
    "flink_checkpoint_failure",     # job runs but checkpoints fail or stall
    "pipeline_lag",                 # consumer lag or backpressure without a component failure
    "iceberg_maintenance",          # small files, overdue compaction, snapshot build-up
    "node_or_zone_failure",         # a node or a whole zone is lost
    "ingestion_errors",             # the ingestion API fails requests or is slow
    "deployment_regression",        # a recent change broke something
    "gitops_sync_failure",          # Argo CD cannot sync or an app is degraded
    "transient_or_resolved",        # the condition already cleared; nothing is wrong now
    "unknown",
)

Category = Literal[CATEGORIES]  # type: ignore[valid-type]
ActionName = Literal[tuple(ACTIONS)]  # type: ignore[valid-type]


class Evidence(BaseModel):
    # ops-control refuses a proposal whose evidence names a tool that was not called, so the name must be exact.
    tool: str = Field(description="The exact name of a tool you called, e.g. get_pods; no arguments (put those in finding)")
    finding: str = Field(description="What the result showed, with the numbers")


class Parameters(BaseModel):
    namespace: str = Field(description="restart_pod only; otherwise empty")
    pod: str = Field(description="restart_pod only; otherwise empty")
    table: str = Field(description="run_iceberg_compaction only; otherwise empty")


class ProposedAction(BaseModel):
    action: ActionName
    parameters: Parameters
    rationale: str = Field(description="Why this action, and why it is safe now")


class Diagnosis(BaseModel):
    summary: str = Field(description="One or two sentences for the on-call engineer")
    root_cause_category: Category
    root_cause: str
    confidence: Literal["low", "medium", "high"]
    impact: str = Field(description="What users or data are affected, and how much")
    evidence: list[Evidence]
    proposed_action: ProposedAction
    runbook: str = Field(description="The runbook section used, e.g. docs/runbooks/kafka.md#kafkabrokerdown")
    follow_ups: list[str] = Field(description="Things a human should check or change later")


def _strict(schema: dict[str, Any]) -> dict[str, Any]:
    """Structured outputs require every property listed in `required` and no extra properties."""
    if schema.get("type") == "object":
        schema["additionalProperties"] = False
        schema["required"] = list(schema.get("properties", {}))
    for value in schema.values():
        if isinstance(value, dict):
            _strict(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _strict(item)
    return schema


def for_tools(tool_names: tuple[str, ...]) -> type[Diagnosis]:
    """The diagnosis with `evidence.tool` limited to these tools: structured output cannot then name anything else."""
    if not tool_names:
        return Diagnosis
    evidence = create_model("Evidence", __base__=Evidence, tool=(Literal[tool_names],  # type: ignore[valid-type]
                                                                Field(description=Evidence.model_fields["tool"].description)))
    return create_model("Diagnosis", __base__=Diagnosis, evidence=(list[evidence], ...))  # type: ignore[valid-type]


def output_schema() -> dict[str, Any]:
    schema = Diagnosis.model_json_schema()
    assert tuple(Parameters.model_fields) == PARAMETER_NAMES
    return _strict(schema)
