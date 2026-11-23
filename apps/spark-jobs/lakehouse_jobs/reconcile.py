"""Offset-level reconciliation: every Kafka record in the window must be in events or events_skipped exactly once.

The ingestion producer is idempotent and not transactional, so each partition's offsets are dense: a missing
offset is a record Kafka acknowledged (the API answered 200) that never reached the lakehouse.
"""
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_jobs.common import build_session, emit_summary, utc_timestamp_literal

DEFAULT_EVENTS = "lakehouse.lakehouse.events"
DEFAULT_SKIPPED = "lakehouse.lakehouse.events_skipped"
DEFAULT_TARGET = "lakehouse.lakehouse.events_reconciliation"
# Snapshot summary keys the iceberg-exporter turns into gauges (iceberg_table_snapshot_property).
PROPERTY_PREFIX = "lakehouse."
# Set by the replay tool (io.lakehouse.flink.ReplayTool): "<partition>:<offset>:<kafka timestamp ms>" of the record
# an event replaces, so the replayed copy accounts for the original offset.
REPLAY_MARKER = "lakehouse.replayed_from"
MAX_RANGES = 100

TARGET_DDL = """
CREATE TABLE IF NOT EXISTS {target} (
  run_at              timestamp,
  window_start        timestamp,
  window_end          timestamp,
  partitions          int,
  checked_offsets     bigint,
  stored              bigint,
  skipped_duplicate   bigint,
  skipped_undecodable bigint,
  missing             bigint,
  duplicated_offsets  bigint,
  duplicate_event_ids bigint,
  missing_ranges      array<string>,
  details             array<struct<hour: timestamp, kafka_partition: int, first_offset: bigint, last_offset: bigint,
                                   stored: bigint, skipped_duplicate: bigint, skipped_undecodable: bigint,
                                   missing: bigint, stored_by_source: map<string, bigint>, replayed: bigint>>,
  replayed            bigint
) USING iceberg
PARTITIONED BY (days(run_at))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""


def window_offsets(spark: SparkSession, events: str, skipped: str, start: datetime, end: datetime,
                   slack: timedelta) -> DataFrame:
    """Records whose Kafka timestamp is within `slack` of [start, end), in one scan of each table (the filter lets
    Iceberg skip files). Rows written before the lineage columns existed are left out."""
    lo, hi = utc_timestamp_literal(start - slack), utc_timestamp_literal(end + slack)
    where = f"kafka_offset IS NOT NULL AND kafka_timestamp >= TIMESTAMP '{lo}' AND kafka_timestamp < TIMESTAMP '{hi}'"
    marker = f"split(attributes['{REPLAY_MARKER}'], ':')"
    # A replay is written after its original, so its markers are read from the window start on, with no end.
    return spark.sql(f"""
        SELECT kafka_partition AS p, kafka_offset AS o, kafka_timestamp AS ts, source, event_id, 'stored' AS kind
        FROM {events} WHERE {where}
        UNION ALL
        SELECT kafka_partition, kafka_offset, kafka_timestamp, source, event_id, reason FROM {skipped} WHERE {where}
        UNION ALL
        SELECT CAST({marker}[0] AS int), CAST({marker}[1] AS bigint),
               timestamp_millis(CAST({marker}[2] AS bigint)), source, event_id, 'replayed'
        FROM {events}
        WHERE kafka_timestamp >= TIMESTAMP '{lo}' AND attributes['{REPLAY_MARKER}'] IS NOT NULL""")


def offset_ranges(offsets: DataFrame, start: datetime, end: datetime) -> list:
    """Per partition, the first and last offset whose Kafka timestamp is in [start, end): [(p, lo, hi), ...]."""
    lo, hi = utc_timestamp_literal(start), utc_timestamp_literal(end)
    return [(r["p"], r["lo"], r["hi"]) for r in
            offsets.where(f"ts >= TIMESTAMP '{lo}' AND ts < TIMESTAMP '{hi}'")
            .groupBy("p").agg(F.min("o").alias("lo"), F.max("o").alias("hi")).collect()]


def in_ranges(offsets: DataFrame, ranges: list) -> DataFrame:
    """Every record inside the ranges, whatever its timestamp: several producers write to a partition, so
    timestamps are not in offset order and a timestamp cut would leave holes that are not losses. Records more
    than the slack out of order are outside `offsets` and would show as missing."""
    if not ranges:
        return offsets.limit(0)
    condition = " OR ".join(f"(p = {p} AND o BETWEEN {lo} AND {hi})" for p, lo, hi in ranges)
    return offsets.where(condition)


def with_replays(offsets: DataFrame) -> DataFrame:
    """Replay markers count for offsets that have no row of their own (a record lost after Kafka). An undecodable
    record that was replayed keeps its own row; its marker is dropped so the offset is not counted twice."""
    # Window functions, not a self-join: Spark re-plans one side of a self-join from the source tables.
    has_row = F.max(F.when(F.col("kind") != "replayed", 1).otherwise(0)).over(Window.partitionBy("p", "o"))
    nth = F.row_number().over(Window.partitionBy("p", "o", "kind").orderBy("event_id"))
    return (offsets.withColumn("_has_row", has_row).withColumn("_nth", nth)
            .where("kind <> 'replayed' OR (_has_row = 0 AND _nth = 1)").drop("_has_row", "_nth"))


def reconcile(offsets: DataFrame, start: datetime, end: datetime) -> dict:
    """`offsets` (after `with_replays`) should be materialised: everything here derives from it."""
    ranges = offset_ranges(offsets, start, end)
    rows = in_ranges(offsets, ranges).cache()
    try:
        expected = sum(hi - lo + 1 for _, lo, hi in ranges)
        counts = rows.groupBy("kind").count().collect()
        by_kind = {r["kind"]: r["count"] for r in counts}
        distinct = rows.select("p", "o").distinct()
        present = distinct.count()
        duplicated = rows.groupBy("p", "o").count().where("count > 1").count()
        stored = rows.where("kind = 'stored'")
        duplicate_ids = stored.count() - stored.select("event_id").distinct().count()

        # A hole before offset o: o - previous offset - 1 records missing, counted in o's hour.
        w = Window.partitionBy("p").orderBy("o")
        first_seen = rows.groupBy("p", "o").agg(F.min("ts").alias("ts"))
        gaps = (first_seen.withColumn("prev", F.lag("o").over(w))
                .withColumn("gap", F.col("o") - F.col("prev") - 1)
                .where("gap > 0"))
        missing_ranges = [f"p{r['p']}:{r['prev'] + 1}-{r['o'] - 1}"
                          for r in gaps.orderBy("p", "o").limit(MAX_RANGES).collect()]
        missing_by_hour = (gaps.groupBy("p", F.date_trunc("hour", "ts").alias("hour"))
                           .agg(F.sum("gap").alias("missing")))

        per_source = (stored.groupBy("p", F.date_trunc("hour", "ts").alias("hour"), "source").count()
                      .groupBy("p", "hour")
                      .agg(F.map_from_entries(F.collect_list(F.struct("source", "count"))).alias("stored_by_source")))
        detail = (rows.groupBy("p", F.date_trunc("hour", "ts").alias("hour"))
                  .agg(F.min("o").alias("first_offset"), F.max("o").alias("last_offset"),
                       F.count(F.when(F.col("kind") == "stored", 1)).alias("stored"),
                       F.count(F.when(F.col("kind") == "duplicate", 1)).alias("skipped_duplicate"),
                       F.count(F.when(F.col("kind") == "undecodable", 1)).alias("skipped_undecodable"),
                       F.count(F.when(F.col("kind") == "replayed", 1)).alias("replayed"))
                  .join(missing_by_hour, ["p", "hour"], "left")
                  .join(per_source, ["p", "hour"], "left")
                  .select("hour", F.col("p").alias("kafka_partition"), "first_offset", "last_offset", "stored",
                          "skipped_duplicate", "skipped_undecodable",
                          F.coalesce("missing", F.lit(0)).cast("bigint").alias("missing"),
                          F.coalesce("stored_by_source", F.create_map().cast("map<string,bigint>"))
                          .alias("stored_by_source"), "replayed")
                  .orderBy("hour", "kafka_partition"))
        return {
            "partitions": len(ranges),
            "checked_offsets": int(expected),
            "stored": by_kind.get("stored", 0),
            "skipped_duplicate": by_kind.get("duplicate", 0),
            "skipped_undecodable": by_kind.get("undecodable", 0),
            "replayed": by_kind.get("replayed", 0),
            "missing": int(expected) - present,
            "duplicated_offsets": duplicated,
            "duplicate_event_ids": duplicate_ids,
            "missing_ranges": missing_ranges,
            "details": [r.asDict(recursive=True) for r in detail.collect()],
        }
    finally:
        rows.unpersist()


def ensure_replayed_columns(spark: SparkSession, target: str) -> None:
    """Tables created before replays were counted get the column (metadata only; older runs read null)."""
    schema = spark.table(target).schema
    if "replayed" not in schema.fieldNames():
        spark.sql(f"ALTER TABLE {target} ADD COLUMN replayed bigint")
    if "replayed" not in schema["details"].dataType.elementType.fieldNames():
        spark.sql(f"ALTER TABLE {target} ADD COLUMN details.element.replayed bigint")


def write_result(spark: SparkSession, target: str, run_at: datetime, start: datetime, end: datetime,
                 result: dict, expected_interval_seconds: int = 0) -> None:
    spark.sql(TARGET_DDL.format(target=target))
    ensure_replayed_columns(spark, target)
    schema = spark.table(target).schema
    row = {"run_at": run_at, "window_start": start, "window_end": end,
           **{k: result[k] for k in result}}
    df = spark.createDataFrame([tuple(row[f.name] for f in schema.fields)], schema)
    writer = df.writeTo(target)
    # One snapshot per run carries the totals, so the exporter needs no table scan.
    for key in ("checked_offsets", "missing", "duplicated_offsets", "skipped_undecodable", "skipped_duplicate",
                "duplicate_event_ids", "replayed"):
        writer = writer.option(f"snapshot-property.{PROPERTY_PREFIX}{key.replace('_', '-')}", str(result[key]))
    # When the next run is due (0: a one-off run); the staleness alert reads it, so manual runs never trip it.
    writer = writer.option(f"snapshot-property.{PROPERTY_PREFIX}expected-interval-seconds",
                           str(expected_interval_seconds))
    writer.append()


def run(spark: SparkSession, events: str, skipped: str, target: str, start: datetime, end: datetime,
        expected_interval_seconds: int = 0, slack: timedelta = timedelta(hours=1)) -> dict:
    if end <= start:
        raise ValueError("end must be after start")
    started = time.monotonic()
    # Read once and keep it: with persist(), Spark re-resolved the tables for each derived query and so missed the
    # cache, scanning again at a newer snapshot. A local checkpoint keeps no table in the plan to re-resolve.
    offsets = with_replays(window_offsets(spark, events, skipped, start, end, slack)).localCheckpoint(
        eager=True, storageLevel=StorageLevel.MEMORY_AND_DISK)
    result = reconcile(offsets, start, end)
    run_at = datetime.now(timezone.utc)
    write_result(spark, target, run_at, start, end, result, expected_interval_seconds)
    summary = {k: v for k, v in result.items() if k != "details"}
    summary.update({"target": target, "window_start": start.isoformat(), "window_end": end.isoformat(),
                    "seconds": round(time.monotonic() - started, 1)})
    return summary


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--events", default=DEFAULT_EVENTS)
    p.add_argument("--skipped", default=DEFAULT_SKIPPED)
    p.add_argument("--target", default=DEFAULT_TARGET)
    p.add_argument("--lookback-hours", type=float, default=24, help="Kafka timestamps from this long ago")
    p.add_argument("--settle-minutes", type=float, default=15,
                   help="leave out the newest records, which Flink may not have committed yet")
    p.add_argument("--timestamp-slack-minutes", type=float, default=60,
                   help="how far out of offset order Kafka timestamps may be (producer clock skew)")
    p.add_argument("--expected-interval-minutes", type=float, default=0,
                   help="how often the schedule runs this job; 0 for a one-off run (disarms the staleness alert)")
    p.add_argument("--start", help="ISO timestamp; with --end overrides the lookback")
    p.add_argument("--end", help="ISO timestamp (exclusive)")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    if args.start or args.end:
        if not (args.start and args.end):
            raise SystemExit("--start and --end must be given together")
        start, end = _parse_time(args.start), _parse_time(args.end)
    else:
        end = now - timedelta(minutes=args.settle_minutes)
        start = end - timedelta(hours=args.lookback_hours)
    spark = build_session("lakehouse-reconcile")
    summary = run(spark, args.events, args.skipped, args.target, start, end,
                  int(args.expected_interval_minutes * 60), timedelta(minutes=args.timestamp_slack_minutes))
    emit_summary("reconcile", summary)
    spark.stop()
