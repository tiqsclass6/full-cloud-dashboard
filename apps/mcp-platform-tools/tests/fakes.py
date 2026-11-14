"""A fake `Sources` with a small, realistic cluster state: one broker down, Flink running."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from platform_tools.sources import SourceError

NOW = time.time()


def ago(minutes: float) -> datetime:
    return datetime.fromtimestamp(NOW - minutes * 60, tz=timezone.utc)


def pod(name: str, ready: bool = True, restarts: int = 0, node: str = "lakehouse-worker", labels=None,
        terminated: str | None = None) -> dict[str, Any]:
    status = {"name": "main", "ready": ready, "restart_count": restarts, "state": {"running": {}},
              "last_state": {}}
    if terminated:
        status["last_state"] = {"terminated": {"reason": terminated, "exit_code": 137, "finished_at": ago(3)}}
    return {"metadata": {"name": name, "creation_timestamp": ago(120), "labels": labels or {}},
            "spec": {"node_name": node}, "status": {"phase": "Running", "container_statuses": [status]}}


class FakeSources:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.log_text = "INFO started\nERROR connect failed password=hunter2 to broker-1\n"
        self.prom: dict[str, list[dict[str, Any]]] = {
            'up{job="monitoring/kafka-brokers"}': [
                {"metric": {"pod": "lakehouse-dual-role-0"}, "value": [NOW, "1"]},
                {"metric": {"pod": "lakehouse-dual-role-1"}, "value": [NOW, "0"]},
                {"metric": {"pod": "lakehouse-dual-role-2"}, "value": [NOW, "1"]},
            ],
            "sum(kafka_server_replicamanager_underreplicatedpartitions)": [{"metric": {}, "value": [NOW, "12"]}],
            "sum(kafka_controller_kafkacontroller_activecontrollercount)": [{"metric": {}, "value": [NOW, "1"]}],
            "sum by (consumergroup) (kafka_consumergroup_lag >= 0)": [
                {"metric": {"consumergroup": "flink-events-to-iceberg"}, "value": [NOW, "800"]}],
            "iceberg_table_small_data_files": [{"metric": {"table": "lakehouse.events"}, "value": [NOW, "701"]}],
            "iceberg_table_last_compaction_timestamp_seconds": [
                {"metric": {"table": "lakehouse.events"}, "value": [NOW, "NaN"]}],
            "iceberg_table_last_commit_timestamp_seconds": [
                {"metric": {"table": "lakehouse.events"}, "value": [NOW, str(NOW - 30)]}],
        }

    def prom_query(self, query: str):
        self.calls.append(("prom_query", (query,)))
        return self.prom.get(query, [])

    def prom_query_range(self, query, start, end, step):
        self.calls.append(("prom_query_range", (query, start, end, step)))
        return [{"metric": {"pod": "a"}, "values": [[end - 60 * i, str(i)] for i in range(100, 0, -1)]}]

    def alerts(self):
        return [
            {"labels": {"alertname": "Watchdog", "severity": "none"}, "annotations": {}, "startsAt": ago(500).isoformat()},
            {"labels": {"alertname": "KafkaBrokerDown", "severity": "critical", "namespace": "kafka"},
             "annotations": {"summary": "Fewer than 3 brokers up",
                             "runbook_url": "docs/runbooks/kafka.md#kafkabrokerdown"},
             "startsAt": ago(4).isoformat()},
        ]

    def loki_query(self, query, start, end, limit):
        return [{"stream": {"namespace": "kafka", "pod": "lakehouse-dual-role-1", "job": "x"},
                 "values": [[str(int((end - 5) * 1e9)), "WARN token=abc123 rejected"],
                            [str(int((end - 65) * 1e9)), "INFO ok"]]}]

    def pods(self, namespace, label_selector=""):
        self.calls.append(("pods", (namespace, label_selector)))
        if namespace == "kafka":
            return [pod("lakehouse-dual-role-0"), pod("lakehouse-dual-role-1", ready=False, restarts=2, terminated="OOMKilled"),
                    pod("lakehouse-dual-role-2")]
        return [pod("events-to-iceberg-taskmanager-1-1", labels={"component": "taskmanager"})]

    def events(self, namespace):
        return [{"type": "Warning", "reason": "Unhealthy", "message": "Readiness probe failed",
                 "involved_object": {"kind": "Pod", "name": "lakehouse-dual-role-1"}, "count": 5,
                 "last_timestamp": ago(2)},
                {"type": "Normal", "reason": "Pulled", "message": "ok",
                 "involved_object": {"kind": "Pod", "name": "x"}, "last_timestamp": ago(1)},
                {"type": "Warning", "reason": "Old", "message": "old",
                 "involved_object": {"kind": "Pod", "name": "y"}, "last_timestamp": ago(600)}]

    def pod_log(self, namespace, pod, container, tail, previous):
        return self.log_text

    def nodes(self):
        return [{"metadata": {"name": "lakehouse-worker3", "labels": {"topology.kubernetes.io/zone": "zone-c"}},
                 "spec": {"taints": [{"key": "node.kubernetes.io/unreachable", "effect": "NoSchedule"}]},
                 "status": {"conditions": [{"type": "Ready", "status": "Unknown"},
                                           {"type": "MemoryPressure", "status": "False"}]}}]

    def custom(self, group, version, namespace, plural, name):
        if plural == "kafkas":
            return {"status": {"kafkaVersion": "4.3.1", "conditions": [{"type": "Ready", "status": "True"}]}}
        if plural == "flinkdeployments":
            return {"spec": {"job": {"state": "running"}},
                    "status": {"jobStatus": {"state": "RUNNING", "startTime": str(int((NOW - 600) * 1000))},
                               "lifecycleState": "STABLE", "jobManagerDeploymentStatus": "READY"}}
        raise SourceError("Kubernetes API 404: Not Found")

    def custom_list(self, group, version, namespace, plural):
        return [{"metadata": {"name": "platform-local"},
                 "status": {"sync": {"status": "Synced", "revision": "abcdef1234567890"},
                            "health": {"status": "Healthy"},
                            "history": [{"revision": "abcdef1234567890",
                                         "deployedAt": (ago(30)).isoformat()},
                                        {"revision": "0123", "deployedAt": (ago(60 * 48)).isoformat()}]}}]
