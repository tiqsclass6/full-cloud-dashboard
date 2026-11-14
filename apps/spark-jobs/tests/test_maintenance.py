import os
from datetime import datetime, timedelta, timezone

import pytest

from helpers import append, count, event, ids
from lakehouse_jobs import maintenance
from lakehouse_jobs.maintenance import Options


def make_small_files(spark, table, appends=12, rows_each=5):
    for _ in range(appends):
        append(spark, table, [event() for _ in range(rows_each)])


def test_compaction_merges_small_files_and_preserves_every_row(spark, events_table):
    make_small_files(spark, events_table)
    before_ids = ids(spark, events_table)

    result = maintenance.run(spark, events_table, Options(steps=("compact",), min_input_files=5))

    assert result["before"]["data_files"] >= 12
    assert result["after"]["data_files"] < result["before"]["data_files"]
    assert result["after"]["data_files"] <= 2
    assert result["steps"]["compact"]["rewritten_data_files_count"] >= 12
    assert result["after"]["snapshots"] == result["before"]["snapshots"] + 1  # one rewrite commit
    assert ids(spark, events_table) == before_ids  # nothing lost, nothing duplicated
    assert result["after"]["rows"] == result["before"]["rows"] == 60


def test_maintenance_works_when_the_namespace_is_named_like_the_catalog(spark):
    """Production layout: catalog `lakehouse`, namespace `lakehouse`. A two-part procedure argument breaks here."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.lakehouse")
    table = "lakehouse.lakehouse.events_same_name"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        event_id string, source string, type string, event_time timestamp, ingest_time timestamp,
        value double, attributes map<string,string>, region string)
        USING iceberg PARTITIONED BY (days(event_time))""")
    try:
        make_small_files(spark, table, appends=6)

        result = maintenance.run(spark, table, Options(
            steps=("compact", "manifests", "expire"), min_input_files=5, expire_older_than_hours=0, retain_last=1))

        assert result["steps"]["compact"]["rewritten_data_files_count"] >= 6
        assert result["after"]["rows"] == 30
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_compaction_is_idempotent(spark, events_table):
    make_small_files(spark, events_table)
    maintenance.run(spark, events_table, Options(steps=("compact",), min_input_files=5))

    second = maintenance.run(spark, events_table, Options(steps=("compact",), min_input_files=5))

    assert second["steps"]["compact"]["rewritten_data_files_count"] == 0
    assert second["after"]["snapshots"] == second["before"]["snapshots"]


def test_compaction_leaves_partitions_with_few_files_alone(spark, events_table):
    make_small_files(spark, events_table, appends=3)

    result = maintenance.run(spark, events_table, Options(steps=("compact",), min_input_files=5))

    assert result["steps"]["compact"]["rewritten_data_files_count"] == 0
    assert result["after"]["data_files"] == result["before"]["data_files"]


def test_expire_drops_old_snapshots_but_keeps_current_data(spark, events_table):
    make_small_files(spark, events_table)
    maintenance.run(spark, events_table, Options(steps=("compact",), min_input_files=5))
    before = maintenance.table_stats(spark, events_table, 32)
    rows = count(spark, events_table)
    two_days_later = datetime.now(timezone.utc) + timedelta(days=2)

    result = maintenance.run(spark, events_table, Options(
        steps=("expire",), expire_older_than_hours=24, retain_last=1, now=lambda: two_days_later))

    assert before["snapshots"] > 1
    assert result["after"]["snapshots"] == 1
    assert result["steps"]["expire"]["deleted_data_files_count"] >= 12  # the pre-compaction small files
    assert count(spark, events_table) == rows


def test_expire_keeps_recent_snapshots(spark, events_table):
    make_small_files(spark, events_table, appends=4)
    before = maintenance.table_stats(spark, events_table, 32)

    result = maintenance.run(spark, events_table, Options(
        steps=("expire",), expire_older_than_hours=24, retain_last=1))

    assert result["after"]["snapshots"] == before["snapshots"]  # all are younger than 24 h


def _table_location(spark, table):
    rows = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
    return [r["data_type"] for r in rows if r["col_name"] == "Location"][0].replace("file:", "")


def test_orphan_removal_only_touches_old_unreferenced_files(spark, events_table):
    make_small_files(spark, events_table, appends=2)
    data_dir = os.path.join(_table_location(spark, events_table), "data")
    os.makedirs(data_dir, exist_ok=True)
    recent = os.path.join(data_dir, "unreferenced-recent.parquet")  # could belong to an in-flight write
    old = os.path.join(data_dir, "unreferenced-old.parquet")  # left behind by a failed write days ago
    for path in (recent, old):
        with open(path, "wb") as handle:
            handle.write(b"not referenced by any snapshot")
    five_days_ago = (datetime.now(timezone.utc) - timedelta(days=5)).timestamp()
    os.utime(old, (five_days_ago, five_days_ago))
    rows = count(spark, events_table)

    result = maintenance.run(spark, events_table, Options(steps=("orphans",), orphan_older_than_hours=72))

    assert not os.path.exists(old)
    assert os.path.exists(recent), "a fresh unreferenced file may belong to an in-flight write and must stay"
    assert result["steps"]["orphans"]["orphan_files_removed"] == 1
    assert count(spark, events_table) == rows  # referenced data files were not touched


def test_orphan_window_shorter_than_a_day_is_refused_by_iceberg(spark, events_table):
    make_small_files(spark, events_table, appends=1)

    with pytest.raises(Exception, match="less than 24 hours"):
        maintenance.run(spark, events_table, Options(steps=("orphans",), orphan_older_than_hours=1))


def test_manifest_rewrite_runs_and_keeps_data(spark, events_table):
    make_small_files(spark, events_table, appends=6)
    rows = count(spark, events_table)

    result = maintenance.run(spark, events_table, Options(steps=("manifests",)))

    assert "manifests" in result["steps"]
    assert count(spark, events_table) == rows


def test_run_reports_statistics_and_optional_scan_benchmark(spark, events_table):
    make_small_files(spark, events_table, appends=2)

    result = maintenance.run(spark, events_table,
                             Options(steps=("compact",), benchmark_query=True, benchmark_runs=2))

    for key in ("data_files", "small_files", "total_mib", "avg_file_mib", "rows", "snapshots", "manifests"):
        assert key in result["before"] and key in result["after"]
    assert result["scan_before"]["runs"] == 2 and result["scan_after"]["median_seconds"] >= 0
    assert result["total_seconds"] >= 0


def test_unknown_step_is_rejected_before_anything_runs(spark, events_table):
    make_small_files(spark, events_table, appends=2)
    snapshots = maintenance.table_stats(spark, events_table, 32)["snapshots"]

    with pytest.raises(ValueError, match="unknown steps"):
        maintenance.run(spark, events_table, Options(steps=("compact", "vacuum")))

    assert maintenance.table_stats(spark, events_table, 32)["snapshots"] == snapshots


def test_steps_run_in_a_safe_fixed_order(spark, events_table):
    make_small_files(spark, events_table)

    result = maintenance.run(spark, events_table, Options(steps=("expire", "compact"), min_input_files=5))

    assert list(result["steps"]) == ["compact", "expire"]  # compaction first, then expiry frees old files


def test_dedupe_keeps_one_row_per_event_id_and_is_idempotent(spark, events_table):
    from helpers import ts
    originals = [event(event_id=f"id-{i}", when=ts(28, 10, i)) for i in range(20)]
    other_day = [event(event_id=f"day2-{i}", when=ts(29, 9, i)) for i in range(5)]
    replayed = [event(event_id=f"id-{i}", when=ts(28, 10, i)) for i in range(10)]  # a stream replay
    append(spark, events_table, originals + other_day)
    append(spark, events_table, replayed)

    result = maintenance.run(spark, events_table, Options(steps=("dedupe",)))

    step = result["steps"]["dedupe"]
    assert (step["rows_before"], step["duplicate_rows"], step["rows_after"]) == (35, 10, 25)
    assert ids(spark, events_table) == sorted([f"id-{i}" for i in range(20)] + [f"day2-{i}" for i in range(5)])

    again = maintenance.run(spark, events_table, Options(steps=("dedupe",)))
    assert again["steps"]["dedupe"]["rewritten"] is False
    assert again["after"]["snapshots"] == again["before"]["snapshots"]


def test_dedupe_removes_a_copy_that_is_alone_in_another_day_partition(spark, events_table):
    from helpers import ts
    # Same event_id, but the replayed copy carries an event_time on the next day (days(event_time) partition).
    original = event(event_id="cross-day", when=ts(28, 23, 59), value=1.0)
    other_day_copy = ("cross-day", "s1", "cpu", ts(29, 0, 1), ts(29, 0, 1), 1.0, {"k": "v"}, None)
    append(spark, events_table, [original, event(event_id="keep", when=ts(28, 10))])
    append(spark, events_table, [other_day_copy])

    result = maintenance.run(spark, events_table, Options(steps=("dedupe",)))

    step = result["steps"]["dedupe"]
    assert (step["rows_before"], step["duplicate_rows"], step["rows_after"]) == (3, 1, 2)
    rows = {r["event_id"]: r for r in spark.table(events_table).collect()}
    assert sorted(rows) == ["cross-day", "keep"]
    assert rows["cross-day"]["event_time"].day == 28  # the earliest-ingested copy is the one kept


def test_dedupe_keeps_the_earliest_ingested_copy(spark, events_table):
    from helpers import ts
    first = event(event_id="same", when=ts(28, 10, 1), value=1.0)
    later = event(event_id="same", when=ts(28, 10, 30), value=2.0)
    append(spark, events_table, [later])
    append(spark, events_table, [first])

    maintenance.run(spark, events_table, Options(steps=("dedupe",)))

    rows = spark.table(events_table).collect()
    assert len(rows) == 1 and rows[0]["value"] == 1.0


def test_dedupe_is_never_part_of_the_default_steps():
    assert "dedupe" not in maintenance.ALL_STEPS
    assert "dedupe" not in maintenance.Options().steps
    assert maintenance.STEP_ORDER[0] == "dedupe"  # when asked for, it runs before compaction
