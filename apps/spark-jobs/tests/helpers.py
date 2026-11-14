import itertools
from datetime import datetime, timezone

_ids = itertools.count()

EVENT_SCHEMA = (
    "event_id string, source string, type string, event_time timestamp, ingest_time timestamp,"
    " value double, attributes map<string,string>, region string"
)


def ts(day: int, hour: int, minute: int = 0) -> datetime:
    """A fixed UTC moment in October 2026 (tests must not depend on today's date)."""
    return datetime(2026, 10, day, hour, minute, tzinfo=timezone.utc)


def event(source="s1", etype="cpu", when=None, value=1.0, event_id=None):
    when = when or ts(28, 10, 5)
    return (event_id or f"e-{next(_ids)}", source, etype, when, when, value, {"k": "v"}, None)


def append(spark, table, rows):
    spark.createDataFrame(rows, EVENT_SCHEMA).writeTo(table).append()


def count(spark, table):
    return spark.table(table).count()


def ids(spark, table):
    return sorted(r["event_id"] for r in spark.table(table).select("event_id").collect())
