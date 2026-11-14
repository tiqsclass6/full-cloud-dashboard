from datetime import timezone

import pytest

from helpers import append, event, ts
from lakehouse_jobs import aggregate


@pytest.fixture()
def hourly_table(spark, events_table):
    target = events_table.replace("events_", "hourly_")
    yield target
    spark.sql(f"DROP TABLE IF EXISTS {target}")


def utc(moment):
    """Spark hands back naive datetimes (in the UTC session zone); make them comparable with ts()."""
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def rows_of(spark, target):
    result = spark.table(target).orderBy("hour", "source", "type").collect()
    return {(utc(r["hour"]), r["source"], r["type"]): r for r in result}


def run_window(spark, events_table, target, start, end):
    return aggregate.run(spark, events_table, target, start, end)


def test_hour_alignment():
    assert aggregate.hour_floor(ts(28, 10, 59)) == ts(28, 10)
    assert aggregate.hour_ceil(ts(28, 10, 1)) == ts(28, 11)
    assert aggregate.hour_ceil(ts(28, 11)) == ts(28, 11)  # already aligned: unchanged
    assert aggregate.aligned_window(ts(28, 10, 20), ts(28, 11, 10)) == (ts(28, 10), ts(28, 12))
    with pytest.raises(ValueError):
        aggregate.aligned_window(ts(28, 11), ts(28, 10))


def test_computes_correct_hourly_statistics(spark, events_table, hourly_table):
    append(spark, events_table, [
        event("a", "cpu", ts(28, 10, 5), 1.0), event("a", "cpu", ts(28, 10, 30), 3.0),
        event("a", "cpu", ts(28, 10, 59), 5.0), event("b", "cpu", ts(28, 10, 10), 10.0),
        event("a", "cpu", ts(28, 11, 1), 7.0), event("a", "mem", ts(28, 10, 15), 2.0),
    ])

    summary = run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 12))

    rows = rows_of(spark, hourly_table)
    a_cpu_10 = rows[(ts(28, 10), "a", "cpu")]
    assert (a_cpu_10["event_count"], a_cpu_10["value_count"]) == (3, 3)
    assert (a_cpu_10["value_sum"], a_cpu_10["value_min"], a_cpu_10["value_max"]) == (9.0, 1.0, 5.0)
    assert a_cpu_10["value_avg"] == pytest.approx(3.0)
    assert a_cpu_10["value_p95"] in (1.0, 3.0, 5.0)  # an actual observed value (approximate percentile)
    assert rows[(ts(28, 10), "b", "cpu")]["event_count"] == 1
    assert rows[(ts(28, 11), "a", "cpu")]["value_sum"] == 7.0
    assert len(rows) == 4
    assert summary["events_in_window"] == 6 and summary["groups_in_window"] == 4


def test_rerunning_a_window_does_not_duplicate_rows(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 10, 5), 1.0), event("a", "cpu", ts(28, 11, 5), 2.0)])

    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 12))
    first = rows_of(spark, hourly_table)
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 12))
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 12))

    assert spark.table(hourly_table).count() == 2
    assert {k: v["event_count"] for k, v in rows_of(spark, hourly_table).items()} == \
           {k: v["event_count"] for k, v in first.items()}


def test_late_events_correct_an_hour_that_was_already_aggregated(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 10, 5), 1.0)])
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 11))
    assert rows_of(spark, hourly_table)[(ts(28, 10), "a", "cpu")]["event_count"] == 1

    append(spark, events_table, [event("a", "cpu", ts(28, 10, 50), 3.0)])  # arrives late
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 11))

    row = rows_of(spark, hourly_table)[(ts(28, 10), "a", "cpu")]
    assert (row["event_count"], row["value_sum"]) == (2, 4.0)
    assert spark.table(hourly_table).count() == 1


def test_a_group_that_drops_to_zero_events_is_removed_only_inside_the_window(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 10, 5), 1.0), event("b", "cpu", ts(28, 10, 6), 2.0),
                                 event("b", "cpu", ts(28, 12, 6), 2.0)])
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 13))
    assert set(rows_of(spark, hourly_table)) == {(ts(28, 10), "a", "cpu"), (ts(28, 10), "b", "cpu"),
                                                 (ts(28, 12), "b", "cpu")}

    # Every "b" event disappears from the source (e.g. a repair removed them), then hour 10 is recomputed.
    spark.sql(f"DELETE FROM {events_table} WHERE source = 'b'")
    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 11))

    # (10, b) is gone; (12, b) is outside the window and stays until its own hour is recomputed.
    assert set(rows_of(spark, hourly_table)) == {(ts(28, 10), "a", "cpu"), (ts(28, 12), "b", "cpu")}


def test_partial_hour_windows_are_widened_to_whole_hours(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 10, 5), 1.0), event("a", "cpu", ts(28, 11, 55), 2.0)])

    summary = run_window(spark, events_table, hourly_table, ts(28, 10, 40), ts(28, 11, 10))

    assert summary["window_start"].startswith("2026-10-28T10:00")
    assert summary["window_end"].startswith("2026-10-28T12:00")
    assert summary["events_in_window"] == 2  # the 10:05 event is included, so hour 10 is complete


def test_events_outside_the_window_are_ignored(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 9, 5), 1.0), event("a", "cpu", ts(28, 10, 5), 1.0),
                                 event("a", "cpu", ts(28, 12, 0), 1.0)])

    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 12))

    assert list(rows_of(spark, hourly_table)) == [(ts(28, 10), "a", "cpu")]


def test_null_values_count_as_events_but_not_as_measurements(spark, events_table, hourly_table):
    append(spark, events_table, [event("a", "cpu", ts(28, 10, 5), 4.0), event("a", "cpu", ts(28, 10, 6), None)])

    run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 11))

    row = rows_of(spark, hourly_table)[(ts(28, 10), "a", "cpu")]
    assert (row["event_count"], row["value_count"], row["value_avg"]) == (2, 1, 4.0)


def test_an_empty_window_creates_the_table_and_writes_nothing(spark, events_table, hourly_table):
    summary = run_window(spark, events_table, hourly_table, ts(28, 10), ts(28, 11))

    assert spark.table(hourly_table).count() == 0
    assert summary["groups_in_window"] == 0 and summary["events_in_window"] == 0
