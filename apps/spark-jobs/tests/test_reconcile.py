import itertools
from datetime import datetime, timedelta, timezone

import pytest

from lakehouse_jobs import reconcile

T0 = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)
_counter = itertools.count()


@pytest.fixture()
def tables(spark):
    """events (with lineage), events_skipped and the result table, laid out as the Flink job creates them."""
    n = next(_counter)
    names = {k: f"lakehouse.db.{k}_{n}" for k in ("events", "skipped", "target")}
    spark.sql(f"""
        CREATE TABLE {names['events']} (
          event_id string, source string, type string, event_time timestamp, ingest_time timestamp,
          value double, attributes map<string,string>,
          kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp)
        USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""
        CREATE TABLE {names['skipped']} (
          kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, reason string,
          event_id string, source string, error string, payload binary, recorded_at timestamp)
        USING iceberg TBLPROPERTIES ('format-version'='2')""")
    yield names
    for name in names.values():
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _lit(ts: datetime) -> str:
    return "TIMESTAMP '" + ts.strftime("%Y-%m-%d %H:%M:%S") + "'"


def store(spark, table, partition, offset, ts, event_id=None, source="host-1", lineage=True, replayed_from=None):
    event_id = event_id or f"e-{partition}-{offset}"
    p, o, k = (partition, offset, _lit(ts)) if lineage else ("NULL", "NULL", "NULL")
    attrs = f"map('{reconcile.REPLAY_MARKER}', '{replayed_from}')" if replayed_from else "map()"
    spark.sql(f"""INSERT INTO {table} VALUES ('{event_id}', '{source}', 'cpu', {_lit(ts)}, {_lit(ts)}, 1.0,
                  {attrs}, {p}, {o}, {k})""")


def skip(spark, table, partition, offset, ts, reason, event_id=None):
    eid = f"'{event_id}'" if event_id else "NULL"
    spark.sql(f"""INSERT INTO {table} VALUES ({partition}, {offset}, {_lit(ts)}, '{reason}', {eid}, 'host-1',
                  NULL, NULL, {_lit(ts)})""")


def run(spark, t, start=T0, end=T0 + timedelta(hours=2), interval=0):
    return reconcile.run(spark, t["events"], t["skipped"], t["target"], start, end, interval)


def test_a_complete_partition_has_nothing_missing(spark, tables):
    for o in range(5):
        store(spark, tables["events"], 0, o, T0 + timedelta(minutes=o))
    skip(spark, tables["skipped"], 0, 5, T0 + timedelta(minutes=5), "duplicate", "e-0-1")
    skip(spark, tables["skipped"], 0, 6, T0 + timedelta(minutes=6), "undecodable")

    s = run(spark, tables)

    assert (s["checked_offsets"], s["stored"], s["skipped_duplicate"], s["skipped_undecodable"]) == (7, 5, 1, 1)
    assert (s["missing"], s["duplicated_offsets"], s["missing_ranges"]) == (0, 0, [])


def test_reports_each_hole_with_its_offsets(spark, tables):
    for o in [0, 1, 2, 5, 6, 9]:
        store(spark, tables["events"], 3, o, T0 + timedelta(minutes=o))

    s = run(spark, tables)

    assert s["checked_offsets"] == 10
    assert s["missing"] == 4
    assert s["missing_ranges"] == ["p3:3-4", "p3:7-8"]
    row = spark.table(tables["target"]).first()
    assert row["missing"] == 4 and row["details"][0]["missing"] == 4
    assert row["details"][0]["stored_by_source"] == {"host-1": 6}


def test_flags_an_offset_stored_twice(spark, tables):
    for o in range(3):
        store(spark, tables["events"], 1, o, T0 + timedelta(minutes=o))
    store(spark, tables["events"], 1, 1, T0 + timedelta(minutes=1), event_id="e-1-1")

    s = run(spark, tables)

    assert s["duplicated_offsets"] == 1
    assert s["duplicate_event_ids"] == 1
    assert s["missing"] == 0


def test_out_of_order_timestamps_at_the_window_edge_are_not_holes(spark, tables):
    # Offset 1 was written by a producer whose clock is behind: its timestamp falls before the window.
    store(spark, tables["events"], 0, 0, T0 + timedelta(seconds=1))
    store(spark, tables["events"], 0, 1, T0 - timedelta(seconds=1))
    store(spark, tables["events"], 0, 2, T0 + timedelta(seconds=2))

    s = run(spark, tables)

    assert s["checked_offsets"] == 3 and s["missing"] == 0


def test_rows_from_before_the_lineage_columns_are_ignored(spark, tables):
    store(spark, tables["events"], 0, 0, T0, lineage=False)
    store(spark, tables["events"], 0, 10, T0)
    store(spark, tables["events"], 0, 11, T0)

    s = run(spark, tables)

    assert s["checked_offsets"] == 2 and s["missing"] == 0


def test_records_outside_the_window_are_not_checked(spark, tables):
    store(spark, tables["events"], 0, 0, T0 - timedelta(hours=5))
    store(spark, tables["events"], 0, 7, T0 + timedelta(minutes=1))
    store(spark, tables["events"], 0, 8, T0 + timedelta(minutes=2))

    s = run(spark, tables)

    assert s["checked_offsets"] == 2 and s["missing"] == 0


def test_writes_the_totals_into_the_snapshot_summary(spark, tables):
    store(spark, tables["events"], 0, 0, T0)
    store(spark, tables["events"], 0, 2, T0)

    run(spark, tables, interval=3600)

    summary = spark.sql(f"SELECT summary FROM {tables['target']}.snapshots ORDER BY committed_at DESC").first()[0]
    assert summary["lakehouse.missing"] == "1"
    assert summary["lakehouse.checked-offsets"] == "3"
    assert summary["lakehouse.duplicated-offsets"] == "0"
    assert summary["lakehouse.expected-interval-seconds"] == "3600"


def test_an_empty_window_still_records_a_run(spark, tables):
    s = run(spark, tables)

    assert (s["partitions"], s["checked_offsets"], s["missing"]) == (0, 0, 0)
    assert spark.table(tables["target"]).count() == 1


def test_a_timestamp_skewed_past_the_slack_shows_as_missing(spark, tables):
    # The limit of the check: offset 1's timestamp is 2 hours off, beyond the 1-hour slack, so it is not read.
    store(spark, tables["events"], 0, 0, T0 + timedelta(minutes=1))
    store(spark, tables["events"], 0, 1, T0 - timedelta(hours=2))
    store(spark, tables["events"], 0, 2, T0 + timedelta(minutes=2))

    s = run(spark, tables)

    assert s["missing"] == 1 and s["missing_ranges"] == ["p0:1-1"]


def _ms(ts):
    return int(ts.timestamp() * 1000)


def test_a_replay_accounts_for_the_offset_it_replaces(spark, tables):
    store(spark, tables["events"], 0, 0, T0)
    lost = T0 + timedelta(minutes=1)
    store(spark, tables["events"], 0, 2, T0 + timedelta(minutes=2))
    # The replay tool re-sent offset 1 through the API; it came back as offset 3, carrying the marker.
    store(spark, tables["events"], 0, 3, T0 + timedelta(minutes=90), event_id="e-0-1",
          replayed_from=f"0:1:{_ms(lost)}")

    s = run(spark, tables, end=T0 + timedelta(hours=3))

    assert (s["checked_offsets"], s["missing"], s["replayed"], s["duplicated_offsets"]) == (4, 0, 1, 0)
    assert spark.table(tables["target"]).first()["replayed"] == 1


def test_a_replayed_undecodable_record_is_not_counted_twice(spark, tables):
    store(spark, tables["events"], 0, 0, T0)
    skip(spark, tables["skipped"], 0, 1, T0 + timedelta(minutes=1), "undecodable")
    store(spark, tables["events"], 0, 2, T0 + timedelta(minutes=2), event_id="fixed",
          replayed_from=f"0:1:{_ms(T0 + timedelta(minutes=1))}")

    s = run(spark, tables)

    assert (s["checked_offsets"], s["missing"], s["duplicated_offsets"], s["replayed"]) == (3, 0, 0, 0)
    assert s["skipped_undecodable"] == 1


def test_adds_the_replayed_column_to_an_existing_result_table(spark, tables):
    old_ddl = reconcile.TARGET_DDL.replace(", replayed: bigint>>", ">>").replace(",\n  replayed            bigint", "")
    assert "replayed" not in old_ddl
    spark.sql(old_ddl.format(target=tables["target"]))
    store(spark, tables["events"], 0, 0, T0)

    run(spark, tables)

    row = spark.table(tables["target"]).first()
    assert row["replayed"] == 0 and row["details"][0]["replayed"] == 0
