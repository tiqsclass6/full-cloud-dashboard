"""Iceberg table maintenance: compaction, manifest rewrite, snapshot expiry, orphan file removal (72-hour safety window)."""
import argparse
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from pyspark.sql import SparkSession

from lakehouse_jobs.common import (
    build_session,
    catalog_of,
    emit_summary,
    procedure_table,
    utc_timestamp_literal,
)

ALL_STEPS = ("compact", "manifests", "expire", "orphans")
# Repair steps run only when asked for by name, never by default or on a schedule.
REPAIR_STEPS = ("dedupe",)
STEP_ORDER = REPAIR_STEPS + ALL_STEPS
MB = 1024 * 1024


@dataclass
class Options:
    target_file_size_mb: int = 128
    min_input_files: int = 5
    expire_older_than_hours: float = 24
    retain_last: int = 5
    orphan_older_than_hours: float = 72
    steps: tuple = ALL_STEPS
    benchmark_query: bool = False
    benchmark_runs: int = 3
    # Inputs for tests: a clock, so retention can be exercised without waiting.
    now: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc), repr=False)


def table_stats(spark: SparkSession, table: str, small_file_mb: float) -> dict:
    """File and snapshot statistics from Iceberg's metadata tables."""
    files = spark.sql(
        f"""SELECT count(*) AS file_count,
                   coalesce(sum(file_size_in_bytes), 0) AS total_bytes,
                   coalesce(sum(record_count), 0) AS total_rows,
                   coalesce(sum(CASE WHEN file_size_in_bytes < {int(small_file_mb * MB)} THEN 1 ELSE 0 END), 0) AS small_files
            FROM {table}.files""").first()
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").first()["n"]
    manifests = spark.sql(f"SELECT count(*) AS n FROM {table}.manifests").first()["n"]
    count = files["file_count"]
    return {
        "data_files": count,
        "small_files": files["small_files"],
        "total_mib": round(files["total_bytes"] / MB, 1),
        "avg_file_mib": round(files["total_bytes"] / count / MB, 2) if count else 0,
        "rows": files["total_rows"],
        "snapshots": snapshots,
        "manifests": manifests,
    }


def scan_benchmark(spark: SparkSession, table: str, runs: int) -> dict:
    """Median wall time of a representative aggregate scan after one warm-up (a rough indicator)."""
    query = f"SELECT source, type, count(*) AS n, avg(value) AS avg_value FROM {table} GROUP BY source, type"
    spark.sql(query).collect()  # untimed warm-up: the first run pays JIT, executor start-up and cold caches
    timings = []
    for _ in range(runs):
        started = time.monotonic()
        spark.sql(query).collect()
        timings.append(time.monotonic() - started)
    return {"runs": runs, "median_seconds": round(statistics.median(timings), 2),
            "all_seconds": [round(t, 2) for t in timings]}


def compact(spark: SparkSession, table: str, opts: Options) -> dict:
    row = spark.sql(
        f"""CALL {catalog_of(table)}.system.rewrite_data_files(
              table => '{procedure_table(table)}',
              strategy => 'binpack',
              options => map(
                'target-file-size-bytes', '{opts.target_file_size_mb * MB}',
                'min-input-files', '{opts.min_input_files}',
                'partial-progress.enabled', 'true',
                'max-concurrent-file-group-rewrites', '4'))""").first()
    return row.asDict()


def rewrite_manifests(spark: SparkSession, table: str, opts: Options) -> dict:
    return spark.sql(
        f"CALL {catalog_of(table)}.system.rewrite_manifests('{procedure_table(table)}')").first().asDict()


def expire(spark: SparkSession, table: str, opts: Options) -> dict:
    older_than = opts.now() - timedelta(hours=opts.expire_older_than_hours)
    return spark.sql(
        f"""CALL {catalog_of(table)}.system.expire_snapshots(
              table => '{procedure_table(table)}',
              older_than => TIMESTAMP '{utc_timestamp_literal(older_than)}',
              retain_last => {opts.retain_last})""").first().asDict()


def remove_orphans(spark: SparkSession, table: str, opts: Options) -> dict:
    older_than = opts.now() - timedelta(hours=opts.orphan_older_than_hours)
    # prefix_listing uses the catalog's S3FileIO; the default needs a Hadoop S3 connector this image lacks.
    rows = spark.sql(
        f"""CALL {catalog_of(table)}.system.remove_orphan_files(
              table => '{procedure_table(table)}',
              older_than => TIMESTAMP '{utc_timestamp_literal(older_than)}',
              prefix_listing => true)""").collect()
    return {"orphan_files_removed": len(rows)}


def dedupe(spark: SparkSession, table: str, opts: Options) -> dict:
    """REPAIR: keep the earliest row per event_id with a static whole-table overwrite; suspend Flink first."""
    counts = spark.sql(f"SELECT count(*) AS n, count(DISTINCT event_id) AS d FROM {table}").first()
    result = {"rows_before": counts["n"], "distinct_event_ids": counts["d"], "duplicate_rows": counts["n"] - counts["d"]}
    if counts["n"] == counts["d"]:
        return {**result, "rewritten": False}
    columns = ", ".join(f"`{f.name}`" for f in spark.table(table).schema.fields)
    previous = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")  # no PARTITION clause: the whole table
    try:
        spark.sql(f"""
            INSERT OVERWRITE {table}
            SELECT {columns} FROM (
              SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_time, event_time) AS _rn
              FROM {table})
            WHERE _rn = 1""")
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous)
    after = spark.sql(f"SELECT count(*) AS n, count(DISTINCT event_id) AS d FROM {table}").first()
    result.update(rows_after=after["n"], rewritten=True)
    if after["n"] != after["d"] or after["d"] != counts["d"]:
        raise RuntimeError(f"dedupe left {after['n']} rows for {after['d']} distinct event ids "
                           f"(expected {counts['d']} of each): {result}")
    return result


STEP_FUNCTIONS = {"dedupe": dedupe, "compact": compact, "manifests": rewrite_manifests, "expire": expire,
                  "orphans": remove_orphans}


def run(spark: SparkSession, table: str, opts: Options) -> dict:
    """Runs the selected steps in order and returns before/after statistics. Idempotent."""
    unknown = set(opts.steps) - set(STEP_ORDER)
    if unknown:
        raise ValueError(f"unknown steps: {sorted(unknown)}")
    small_mb = opts.target_file_size_mb / 4
    started = time.monotonic()
    result = {"table": table, "before": table_stats(spark, table, small_mb), "steps": {}}
    if opts.benchmark_query:
        result["scan_before"] = scan_benchmark(spark, table, opts.benchmark_runs)
    for step in STEP_ORDER:  # fixed order regardless of how the steps were listed
        if step in opts.steps:
            step_started = time.monotonic()
            outcome = STEP_FUNCTIONS[step](spark, table, opts)
            outcome["seconds"] = round(time.monotonic() - step_started, 1)
            result["steps"][step] = outcome
    result["after"] = table_stats(spark, table, small_mb)
    if opts.benchmark_query:
        result["scan_after"] = scan_benchmark(spark, table, opts.benchmark_runs)
    result["total_seconds"] = round(time.monotonic() - started, 1)
    return result


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--table", default="lakehouse.lakehouse.events", help="catalog.namespace.table")
    p.add_argument("--steps", default=",".join(ALL_STEPS),
                   help=f"comma separated, from: {', '.join(ALL_STEPS)}; repair only (by name): {', '.join(REPAIR_STEPS)}")
    p.add_argument("--target-file-size-mb", type=int, default=128)
    p.add_argument("--min-input-files", type=int, default=5)
    p.add_argument("--expire-older-than-hours", type=float, default=24)
    p.add_argument("--retain-last", type=int, default=5)
    p.add_argument("--orphan-older-than-hours", type=float, default=72)
    p.add_argument("--benchmark-query", action="store_true", help="time a scan before and after")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    opts = Options(
        target_file_size_mb=args.target_file_size_mb,
        min_input_files=args.min_input_files,
        expire_older_than_hours=args.expire_older_than_hours,
        retain_last=args.retain_last,
        orphan_older_than_hours=args.orphan_older_than_hours,
        steps=tuple(s.strip() for s in args.steps.split(",") if s.strip()),
        benchmark_query=args.benchmark_query,
    )
    spark = build_session("lakehouse-maintenance")
    emit_summary("maintenance", run(spark, args.table, opts))
    spark.stop()
