"""The actions ops-control runs after approval; `pin` records the cluster facts the approval covers and `run` rechecks them."""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from ops_agent.catalog import COMPACTION_SCHEDULES

FLINK_NAMESPACE = os.environ.get("FLINK_NAMESPACE", "apps")
FLINK_DEPLOYMENT = os.environ.get("FLINK_DEPLOYMENT", "events-to-iceberg")
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "apps")


def spec_sha256(spec: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class KubeRunner:
    def __init__(self) -> None:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config(context=os.environ.get("KUBE_CONTEXT") or None)
        self.core = client.CoreV1Api()
        self.custom = client.CustomObjectsApi()

    def pin(self, action: str, parameters: dict[str, str]) -> dict[str, str]:
        if action == "run_iceberg_compaction":
            return {"spark_spec_sha256": spec_sha256(self._compaction_template(parameters["table"]))}
        if action == "restart_pod":
            pod = self.core.read_namespaced_pod(parameters["pod"], parameters["namespace"])
            return {"pod_uid": pod.metadata.uid}
        return {}

    def run(self, action: str, parameters: dict[str, str], proposal_id: str, pinned: dict[str, str]) -> str:
        return getattr(self, action)(proposal_id=proposal_id, pinned=pinned, **parameters)

    def restart_pod(self, namespace: str, pod: str, proposal_id: str, pinned: dict[str, str]) -> str:
        from kubernetes import client
        from kubernetes.client.exceptions import ApiException

        approved = pinned.get("pod_uid", "")
        if not approved:
            raise RuntimeError(f"the approval for {namespace}/{pod} pins no pod UID; nothing was deleted")
        current = self.core.read_namespaced_pod(pod, namespace)
        if current.metadata.uid != approved:
            raise RuntimeError(f"{namespace}/{pod} was replaced since the proposal was made (UID {current.metadata.uid},"
                               f" approved {approved}); nothing was deleted")
        owners = current.metadata.owner_references or []
        if not owners:
            raise RuntimeError(f"{namespace}/{pod} has no controller; deleting it would not bring it back")
        # The precondition also covers a pod recreated between the read above and the delete (409).
        try:
            self.core.delete_namespaced_pod(
                pod, namespace, body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=approved)))
        except ApiException as e:
            if e.status == 409:
                raise RuntimeError(f"{namespace}/{pod} was replaced while the action ran; nothing was deleted") from e
            raise
        return f"deleted {namespace}/{pod} (UID {approved}); its {owners[0].kind} {owners[0].name} recreates it"

    def restart_flink_job(self, proposal_id: str, pinned: dict[str, str]) -> str:
        fd = self.custom.get_namespaced_custom_object("flink.apache.org", "v1beta1", FLINK_NAMESPACE,
                                                      "flinkdeployments", FLINK_DEPLOYMENT)
        nonce = int((fd.get("spec") or {}).get("restartNonce") or 0) + 1
        # resourceVersion makes the patch conditional: a concurrent change gets a 409 instead of a false success.
        self.custom.patch_namespaced_custom_object(
            "flink.apache.org", "v1beta1", FLINK_NAMESPACE, "flinkdeployments", FLINK_DEPLOYMENT,
            {"metadata": {"resourceVersion": fd["metadata"]["resourceVersion"]}, "spec": {"restartNonce": nonce}})
        return f"set restartNonce={nonce} on {FLINK_NAMESPACE}/{FLINK_DEPLOYMENT}; the operator restarts the job"

    def run_iceberg_compaction(self, table: str, proposal_id: str, pinned: dict[str, str]) -> str:
        from kubernetes.client.exceptions import ApiException

        schedule = COMPACTION_SCHEDULES[table]
        template = self._compaction_template(table)
        approved = pinned.get("spark_spec_sha256", "")
        if spec_sha256(template) != approved:
            raise RuntimeError(f"the {schedule} schedule changed since the proposal was approved "
                               f"(spec hash {spec_sha256(template)[:12]}, approved {approved[:12]}); nothing was run")
        # One name per proposal: never collides with another proposal, and a repeat of the same one is caught.
        name = f"{schedule}-ops-{proposal_id[:8]}"
        body: dict[str, Any] = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2", "kind": "SparkApplication",
            "metadata": {"name": name, "namespace": SPARK_NAMESPACE,
                         "labels": {"app.kubernetes.io/managed-by": "ops-control", "lakehouse.io/proposal": proposal_id}},
            "spec": template,
        }
        try:
            self.custom.create_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", SPARK_NAMESPACE,
                                                        "sparkapplications", body)
        except ApiException as e:
            if e.status == 409:
                raise RuntimeError(f"SparkApplication {SPARK_NAMESPACE}/{name} already exists for this proposal; "
                                   "not started again") from e
            raise
        return f"started SparkApplication {SPARK_NAMESPACE}/{name} from the {schedule} schedule"

    def _compaction_template(self, table: str) -> dict[str, Any]:
        scheduled = self.custom.get_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", SPARK_NAMESPACE,
                                                             "scheduledsparkapplications", COMPACTION_SCHEDULES[table])
        return scheduled["spec"]["template"]
