"""Settings, read from the environment once at start-up."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv(name: str, default: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in os.environ.get(name, default).split(",") if v.strip())


@dataclass(frozen=True)
class Config:
    prometheus_url: str = "http://kps-prometheus.monitoring.svc:9090"
    alertmanager_url: str = "http://kps-alertmanager.monitoring.svc:9093"
    loki_url: str = "http://loki.observability.svc:3100"
    # RBAC is the real boundary; this list gives the model a clear error instead of a 403.
    namespaces: tuple[str, ...] = (
        "apps", "data", "kafka", "ai-ops", "observability", "monitoring", "argocd",
        "flink-operator", "spark-operator", "strimzi", "kyverno", "external-secrets",
    )
    kafka_namespace: str = "kafka"
    kafka_cluster: str = "lakehouse"
    kafka_broker_job: str = "monitoring/kafka-brokers"
    flink_namespace: str = "apps"
    flink_deployment: str = "events-to-iceberg"
    flink_metric_job: str = "events_to_iceberg"
    flink_consumer_group: str = "flink-events-to-iceberg"
    argocd_namespace: str = "argocd"
    runbook_dir: str = "/opt/runbooks"
    # Every tool answer is cut to this many characters of JSON, so one call cannot flood the context.
    max_result_chars: int = 12_000
    http_timeout_seconds: float = 10.0
    # Optional shared bearer token for the MCP endpoint (network policy is the primary control).
    token: str = field(default="", repr=False)

    @classmethod
    def from_env(cls) -> Config:
        d = cls()
        return cls(
            prometheus_url=os.environ.get("PROMETHEUS_URL", d.prometheus_url),
            alertmanager_url=os.environ.get("ALERTMANAGER_URL", d.alertmanager_url),
            loki_url=os.environ.get("LOKI_URL", d.loki_url),
            namespaces=_csv("ALLOWED_NAMESPACES", ",".join(d.namespaces)),
            kafka_namespace=os.environ.get("KAFKA_NAMESPACE", d.kafka_namespace),
            kafka_cluster=os.environ.get("KAFKA_CLUSTER", d.kafka_cluster),
            kafka_broker_job=os.environ.get("KAFKA_BROKER_JOB", d.kafka_broker_job),
            flink_namespace=os.environ.get("FLINK_NAMESPACE", d.flink_namespace),
            flink_deployment=os.environ.get("FLINK_DEPLOYMENT", d.flink_deployment),
            flink_metric_job=os.environ.get("FLINK_METRIC_JOB", d.flink_metric_job),
            flink_consumer_group=os.environ.get("FLINK_CONSUMER_GROUP", d.flink_consumer_group),
            argocd_namespace=os.environ.get("ARGOCD_NAMESPACE", d.argocd_namespace),
            runbook_dir=os.environ.get("RUNBOOK_DIR", d.runbook_dir),
            max_result_chars=int(os.environ.get("MAX_RESULT_CHARS", d.max_result_chars)),
            http_timeout_seconds=float(os.environ.get("HTTP_TIMEOUT_SECONDS", d.http_timeout_seconds)),
            token=os.environ.get("MCP_TOKEN", ""),
        )
