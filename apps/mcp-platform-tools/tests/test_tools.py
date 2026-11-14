import os
from pathlib import Path

import pytest

from fakes import FakeSources
from platform_tools.config import Config
from platform_tools.redact import redact
from platform_tools.runbooks import RunbookIndex
from platform_tools.tools import PlatformTools, ToolInputError

RUNBOOKS = Path(os.environ.get("RUNBOOK_DIR") or Path(__file__).resolve().parents[3] / "docs" / "runbooks")


@pytest.fixture
def tools():
    return PlatformTools(Config(runbook_dir=str(RUNBOOKS)), FakeSources())


def test_firing_alerts_drop_watchdog_and_keep_runbook(tools):
    out = tools.list_firing_alerts()
    assert out["count"] == 1
    alert = out["alerts"][0]
    assert alert["alertname"] == "KafkaBrokerDown" and alert["severity"] == "critical"
    assert alert["runbook_url"].endswith("#kafkabrokerdown")
    assert 3.9 < alert["firing_for_minutes"] < 4.1


def test_kafka_status_combines_operator_pods_and_metrics(tools):
    out = tools.get_kafka_status()
    assert out["strimzi"]["kafka_version"] == "4.3.1"
    assert out["metrics"]["brokers_scraped_up"] == {
        "lakehouse-dual-role-0": True, "lakehouse-dual-role-1": False, "lakehouse-dual-role-2": True}
    assert out["metrics"]["under_replicated_partitions"] == 12
    assert out["metrics"]["offline_partitions"] is None          # no data is reported as null, not 0
    broker1 = next(b for b in out["brokers"] if b["name"].endswith("-1"))
    assert broker1["ready"] == "0/1" and broker1["last_termination"]["reason"] == "OOMKilled"
    assert out["consumer_lag"] == {"flink-events-to-iceberg": 800}


def test_flink_status_reports_operator_state(tools):
    out = tools.get_flink_job_status()
    assert out["operator"]["job_state"] == "RUNNING"
    assert 9.9 < out["operator"]["job_started_minutes_ago"] < 10.1
    assert out["pods"][0]["component"] == "taskmanager"


def test_iceberg_never_compacted_is_null_not_a_number(tools):
    table = tools.get_iceberg_table_health()["tables"]["lakehouse.events"]
    assert table["small_data_files"] == 701
    assert table["last_compaction_hours_ago"] is None
    assert table["last_commit_minutes_ago"] == 0.5


def test_namespace_allowlist(tools):
    with pytest.raises(ToolInputError, match="not readable"):
        tools.get_pods("kube-system")


def test_events_filter_warnings_and_window(tools):
    out = tools.get_events("kafka", minutes=60)
    assert [e["reason"] for e in out["events"]] == ["Unhealthy"]


def test_logs_are_redacted(tools):
    out = tools.get_pod_logs("kafka", "lakehouse-dual-role-1")
    assert "hunter2" not in str(out) and "[REDACTED]" in out["lines"][1]
    loki = tools.query_loki('{namespace="kafka"}', minutes=5)
    assert loki["lines"][0]["line"] == "WARN token=[REDACTED] rejected"
    assert loki["lines"][0]["seconds_ago"] == 5


def test_loki_requires_a_stream_selector(tools):
    with pytest.raises(ToolInputError, match="stream selector"):
        tools.query_loki("rate(foo[5m])")


def test_prometheus_range_is_downsampled(tools):
    out = tools.query_prometheus("up", range_minutes=100)
    assert len(out["series"][0]["points"]) == 30
    assert out["series"][0]["points"][-1][0] == -60


def test_recent_changes_window(tools):
    out = tools.get_recent_changes(hours=24)
    assert [c["revision"] for c in out["changes"]] == ["abcdef123456"]


def test_nodes(tools):
    node = tools.get_nodes()["nodes"][0]
    assert node == {"name": "lakehouse-worker3", "ready": False, "zone": "zone-c", "problems": [],
                    "unschedulable": False, "taints": ["node.kubernetes.io/unreachable:NoSchedule"]}


def test_runbook_search_exact_alert_and_keywords():
    index = RunbookIndex(RUNBOOKS)
    exact = index.search("KafkaBrokerDown")
    assert len(exact) == 1 and exact[0]["runbook"] == "docs/runbooks/kafka.md#kafkabrokerdown"
    fuzzy = index.search("small files compaction")
    assert fuzzy[0]["heading"] in ("IcebergSmallFilesHigh", "IcebergCompactionOverdue")


def test_runbook_anchors_are_github_slugs():
    index = RunbookIndex(RUNBOOKS)
    terminal = index.search("Approving from a terminal")
    assert terminal[0]["runbook"] == "docs/runbooks/ai-ops.md#approving-from-a-terminal"
    for s in index.sections:     # bold headings, spaces and punctuation never leak into a link
        anchor = s["runbook"].split("#", 1)[1]
        assert "*" not in s["heading"] and anchor and all(c.isalnum() or c in "-_" for c in anchor), s


@pytest.mark.parametrize("text,leak", [
    ("key AKIAABCDEFGHIJKLMNOP used", "AKIAABCDEFGHIJKLMNOP"),
    ("Authorization: Bearer abc.def.ghi", "abc.def.ghi"),
    ("slack xoxb-123456789012-abcdefghij", "xoxb-123456789012"),
    ("jdbc postgres://lakehouse:s3cret@postgres:5432/db", "s3cret"),
    ('{"password": "pw1"}', "pw1"),
    ("AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "wJalrXUtnFEMI"),
    ("export AWS_SESSION_TOKEN=IQoJb3JpZ2luX2VjEAAaCXVz", "IQoJb3JpZ2lu"),
    ("aws_secret_access_key = abc123def", "abc123def"),
    ("db_password: hunter2", "hunter2"),
    ('{"clientSecret": "s3cr3t-value"}', "s3cr3t-value"),
    ("-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY-----", "MIIE"),
])
def test_redaction(text, leak):
    assert leak not in redact(text)


def test_loki_returns_only_streams_from_readable_namespaces(tools):
    tools.sources.loki_query = lambda query, start, end, limit: [
        {"stream": {"namespace": "kafka", "pod": "b-1"}, "values": [[str(int((end - 5) * 1e9)), "kafka line"]]},
        {"stream": {"namespace": "kube-system", "pod": "x"}, "values": [[str(int((end - 5) * 1e9)), "system line"]]},
        {"stream": {"job": "journal"}, "values": [[str(int((end - 5) * 1e9)), "host line"]]},
    ]
    out = tools.query_loki('{job=~".+"}')
    assert [line["line"] for line in out["lines"]] == ["kafka line"]
    assert out["withheld_streams"] == 2 and "allowed" in out["withheld_because"]


def test_prometheus_withholds_series_from_other_namespaces(tools):
    tools.sources.prom_query = lambda query: [
        {"metric": {"namespace": "kafka", "pod": "b-1"}, "value": [0, "1"]},
        {"metric": {"namespace": "kube-system", "pod": "coredns"}, "value": [0, "2"]},
        {"metric": {"exported_namespace": "secrets-ns"}, "value": [0, "3"]},
        {"metric": {"node": "worker"}, "value": [0, "4"]},          # no namespace: numbers only, kept
    ]
    out = tools.query_prometheus("up")
    assert [s["value"] for s in out["series"]] == [1.0, 4.0]
    assert out["withheld_series"] == 2


def _capture_loki(tools):
    seen = []
    tools.sources.loki_query = lambda query, start, end, limit: seen.append(query) or []
    return seen


@pytest.mark.parametrize("query,expected_selector", [
    ('{job=~".+"} |= "error"', '{job=~".+", NS} |= "error"'),
    ('{pod="a}b", container=`c}`} | json', '{pod="a}b", container=`c}`, NS} | json'),
    ("{ }", "{NS}"),
])
def test_loki_puts_the_namespace_scope_into_the_stream_selector(tools, query, expected_selector):
    seen = _capture_loki(tools)
    tools.query_loki(query)
    allowed = "|".join(tools.config.namespaces)
    assert seen == [expected_selector.replace("NS", f'namespace=~"{allowed}"')]


def test_loki_label_format_cannot_relabel_another_namespace_in(tools):
    seen = _capture_loki(tools)
    tools.query_loki('{namespace="kube-system"} | label_format namespace="kafka"')
    # Loki ANDs the two matchers, so nothing from kube-system is read.
    assert seen[0].startswith('{namespace="kube-system", namespace=~"')


def test_loki_rejects_an_unclosed_selector(tools):
    with pytest.raises(ToolInputError, match="not closed"):
        tools.query_loki('{namespace="kafka"')


@pytest.mark.parametrize("query", [
    'label_replace(up{namespace="kube-system"}, "namespace", "kafka", "", "")',
    'label_join(up, "namespace", "", "pod")',
    'up{namespace="langfuse"} * on() group_left(namespace) count by (namespace) (up{namespace="kafka"})',
    "up * on(pod) group_right (namespace, pod) kube_pod_info",
    "info(up)",
])
def test_prometheus_rejects_queries_that_can_rewrite_the_namespace_label(tools, query):
    with pytest.raises(ToolInputError, match="namespace label"):
        tools.query_prometheus(query)


@pytest.mark.parametrize("query", [
    "up",
    "kube_pod_container_status_restarts_total * on(namespace, pod) group_left kube_pod_info",
    "a * on(pod) group_left() b",
    '{__name__="up", pod="label_replace(x)"}',
    "sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))",
])
def test_prometheus_allows_queries_that_keep_the_namespace_label(tools, query):
    tools.query_prometheus(query)
