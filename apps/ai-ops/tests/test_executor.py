"""KubeRunner against a fake Kubernetes API: what is pinned, what runs, and what is refused."""
import copy

import pytest
from kubernetes.client.exceptions import ApiException

from ops_agent.executor import KubeRunner, spec_sha256

TEMPLATE = {"image": "lakehouse/spark-jobs:0.1.0", "arguments": ["--steps", "compact"],
            "driver": {"serviceAccount": "spark-jobs"}}


class FakeCustom:
    def __init__(self) -> None:
        self.template = copy.deepcopy(TEMPLATE)
        self.created: list[dict] = []
        self.patches: list[dict] = []
        self.flink = {"metadata": {"resourceVersion": "41"}, "spec": {"restartNonce": 3}}

    def get_namespaced_custom_object(self, group, version, namespace, plural, name):
        if plural == "scheduledsparkapplications":
            return {"spec": {"template": copy.deepcopy(self.template)}}
        return copy.deepcopy(self.flink)

    def create_namespaced_custom_object(self, group, version, namespace, plural, body):
        if any(c["metadata"]["name"] == body["metadata"]["name"] for c in self.created):
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append(body)

    def patch_namespaced_custom_object(self, group, version, namespace, plural, name, body):
        self.patches.append(body)


@pytest.fixture()
def runner():
    r = KubeRunner.__new__(KubeRunner)      # no cluster connection
    r.custom = FakeCustom()
    return r


def test_compaction_pins_the_live_spec_and_runs_it_under_a_proposal_scoped_name(runner):
    pinned = runner.pin("run_iceberg_compaction", {"table": "lakehouse.events"})
    assert pinned == {"spark_spec_sha256": spec_sha256(TEMPLATE)}

    out = runner.run("run_iceberg_compaction", {"table": "lakehouse.events"}, "1234abcd-0000", pinned)

    body = runner.custom.created[0]
    assert body["metadata"]["name"] == "events-compaction-ops-1234abcd" and body["spec"] == TEMPLATE
    assert body["metadata"]["labels"]["lakehouse.io/proposal"] == "1234abcd-0000"
    assert "events-compaction-ops-1234abcd" in out


def test_compaction_refuses_a_spec_that_changed_after_approval(runner):
    pinned = runner.pin("run_iceberg_compaction", {"table": "lakehouse.events"})
    runner.custom.template["driver"]["serviceAccount"] = "something-else"
    with pytest.raises(RuntimeError, match="changed since the proposal was approved"):
        runner.run("run_iceberg_compaction", {"table": "lakehouse.events"}, "1234abcd-0000", pinned)
    assert runner.custom.created == []


def test_two_proposals_in_the_same_second_get_different_names_and_a_repeat_is_refused(runner):
    pinned = runner.pin("run_iceberg_compaction", {"table": "lakehouse.events"})
    runner.run("run_iceberg_compaction", {"table": "lakehouse.events"}, "aaaaaaaa-1", pinned)
    runner.run("run_iceberg_compaction", {"table": "lakehouse.events"}, "bbbbbbbb-2", pinned)
    assert len(runner.custom.created) == 2
    with pytest.raises(RuntimeError, match="already exists"):
        runner.run("run_iceberg_compaction", {"table": "lakehouse.events"}, "aaaaaaaa-1", pinned)


def test_flink_restart_is_a_conditional_patch(runner):
    out = runner.run("restart_flink_job", {}, "p1", {})
    assert runner.custom.patches == [{"metadata": {"resourceVersion": "41"}, "spec": {"restartNonce": 4}}]
    assert "restartNonce=4" in out


def test_flink_restart_pins_nothing(runner):
    assert runner.pin("restart_flink_job", {}) == {}


class FakeCore:
    """Pods by name; a pod's UID changes when its controller recreates it."""

    def __init__(self) -> None:
        self.uid = "uid-1"
        self.deleted: list[tuple[str, str, object]] = []
        self.conflict = False

    def read_namespaced_pod(self, name, namespace):
        from kubernetes import client

        return client.V1Pod(metadata=client.V1ObjectMeta(
            name=name, namespace=namespace, uid=self.uid,
            owner_references=[client.V1OwnerReference(api_version="v1", kind="StrimziPodSet", name="lakehouse-broker",
                                                      uid="o1")]))

    def delete_namespaced_pod(self, name, namespace, body=None):
        if self.conflict:
            raise ApiException(status=409, reason="Conflict")
        self.deleted.append((namespace, name, body))


@pytest.fixture()
def pod_runner():
    r = KubeRunner.__new__(KubeRunner)
    r.core = FakeCore()
    return r


POD = {"namespace": "kafka", "pod": "lakehouse-broker-1"}


def test_restart_pod_pins_the_uid_and_deletes_with_a_uid_precondition(pod_runner):
    pinned = pod_runner.pin("restart_pod", POD)
    assert pinned == {"pod_uid": "uid-1"}
    out = pod_runner.run("restart_pod", POD, "p1", pinned)
    (namespace, name, body), = pod_runner.core.deleted
    assert (namespace, name) == ("kafka", "lakehouse-broker-1") and body.preconditions.uid == "uid-1"
    assert "uid-1" in out


def test_restart_pod_refuses_a_pod_recreated_after_the_proposal(pod_runner):
    pinned = pod_runner.pin("restart_pod", POD)
    pod_runner.core.uid = "uid-2"           # the controller replaced it before the approval
    with pytest.raises(RuntimeError, match="replaced since the proposal"):
        pod_runner.run("restart_pod", POD, "p1", pinned)
    assert pod_runner.core.deleted == []


def test_restart_pod_reports_a_replacement_between_read_and_delete(pod_runner):
    pinned = pod_runner.pin("restart_pod", POD)
    pod_runner.core.conflict = True         # the API server's precondition check failed
    with pytest.raises(RuntimeError, match="replaced while the action ran"):
        pod_runner.run("restart_pod", POD, "p1", pinned)


def test_restart_pod_without_a_pinned_uid_deletes_nothing(pod_runner):
    with pytest.raises(RuntimeError, match="pins no pod UID"):
        pod_runner.run("restart_pod", POD, "p1", {})
    assert pod_runner.core.deleted == []
