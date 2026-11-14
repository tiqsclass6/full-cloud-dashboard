"""Incremental hourly aggregates of the events table (hour-aligned windows, idempotent MERGE, late data recomputed)."""
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession

from lakehouse_jobs.common import build_session, emit_summary, utc_timestamp_literal

DEFAULT_SOURCE = "lakehouse.lakehouse.events"
DEFAULT_TARGET = "lakehouse.lakehouse.events_hourly"

TARGET_DDL = """
CREATE TABLE IF NOT EXISTS {target} (
  hour        timestamp,
  source      string,
  type        string,
  event_count bigint,
  value_count bigint,
  value_sum   double,
  value_min   double,
  value_max   double,
  value_avg   double,
  value_p95   double
) USING iceberg
PARTITIONED BY (days(hour))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""


def hour_floor(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def hour_ceil(moment: datetime) -> datetime:
    floored = hour_floor(moment)
    return floored if floored == moment.astimezone(timezone.utc) else floored + timedelta(hours=1)


def aligned_window(start: datetime, end: datetime) -> tuple:
    """Widens [start, end) to whole hours."""
    if end <= start:
        raise ValueError("end must be after start")
    return hour_floor(start), hour_ceil(end)


def hourly_query(source: str, start: datetime, end: datetime) -> str:
    """SQL that aggregates [start, end) of the events. Both bounds must already be hour-aligned."""
    lo, hi = utc_timestamp_literal(start), utc_timestamp_literal(end)
    return f"""
        SELECT date_trunc('hour', event_time) AS hour, source, type,
               count(1)                        AS event_count,
               count(value)                    AS value_count,
               sum(value)                      AS value_sum,
               min(value)                      AS value_min,
               max(value)                      AS value_max,
               avg(value)                      AS value_avg,
               percentile_approx(value, 0.95)  AS value_p95
        FROM {source}
        WHERE event_time >= TIMESTAMP '{lo}' AND event_time < TIMESTAMP '{hi}'
        GROUP BY date_trunc('hour', event_time), source, type"""


def run(spark: SparkSession, source: str, target: str, start: datetime, end: datetime) -> dict:
    start, end = aligned_window(start, end)
    started = time.monotonic()
    spark.sql(TARGET_DDL.format(target=target))
    # MERGE from a subquery (a temp view fails to plan on Spark 4.1.4 + Iceberg 1.12.0); emptied groups are deleted.
    lo, hi = utc_timestamp_literal(start), utc_timestamp_literal(end)
    spark.sql(f"""
        MERGE INTO {target} t USING ({hourly_query(source, start, end)}) s
        ON t.hour = s.hour AND t.source = s.source AND t.type = s.type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.hour >= TIMESTAMP '{lo}' AND t.hour < TIMESTAMP '{hi}' THEN DELETE""")
    totals = spark.sql(f"""
        SELECT count(1) AS groups, coalesce(sum(event_count), 0) AS events FROM {target}
        WHERE hour >= TIMESTAMP '{lo}' AND hour < TIMESTAMP '{hi}'""").first()
    target_files = spark.sql(f"SELECT count(*) AS n FROM {target}.files").first()["n"]
    return {
        "source": source,
        "target": target,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "groups_in_window": totals["groups"],
        "events_in_window": totals["events"],
        "target_data_files": target_files,
        "seconds": round(time.monotonic() - started, 1),
        # Recorded so tuning experiments can attribute timing to the settings that were in force.
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
    }


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", default=DEFAULT_SOURCE)
    p.add_argument("--target", default=DEFAULT_TARGET)
    p.add_argument("--lookback-hours", type=float, default=3, help="recompute this many hours up to now")
    p.add_argument("--start", help="ISO timestamp (backfill); with --end overrides --lookback-hours")
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
        start, end = now - timedelta(hours=args.lookback_hours), now
    spark = build_session("lakehouse-hourly-aggregate")
    emit_summary("aggregate", run(spark, args.source, args.target, start, end))
    spark.stop()
