#!/usr/bin/env python3
"""Helpers for local/scripts/integrity-drill.sh (pyiceberg through the port-forwarded REST catalog and S3).

  integrity_drill.py skipped --since 2026-11-19T10:00:00Z         rows of events_skipped since then, by reason
  integrity_drill.py lineage --start 9000000 --count 3000          lineage columns of those eo_test ids
  integrity_drill.py delete-one --event-index 9000123 --save build/row.parquet   the "silent loss"
  integrity_drill.py restore --file build/row.parquet               put the saved row back
  integrity_drill.py old-row --older-than-minutes 75                a stored row Flink's de-duplication has forgotten
  integrity_drill.py event --event-id <id>                          every row of one event, with its replay marker
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from eo_test import event_id  # noqa: E402  (same id scheme as the events the drill sends)

REPLAY_MARKER = "lakehouse.replayed_from"  # set by io.lakehouse.flink.ReplayTool


def catalog(a):
    from pyiceberg.catalog import load_catalog

    return load_catalog("lakehouse", **{
        "type": "rest", "uri": a.catalog_uri,
        "s3.endpoint": a.s3_endpoint, "s3.access-key-id": "lakehouse-local",
        "s3.secret-access-key": "lakehouse-local-secret", "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    })


def with_retries(action, attempts=10):
    """Flink commits every 10 s, so a commit from here can lose the race; reload the table and try again."""
    from pyiceberg.exceptions import CommitFailedException

    for attempt in range(1, attempts + 1):
        try:
            return action()
        except CommitFailedException:
            if attempt == attempts:
                raise
            time.sleep(0.5 * attempt)


def cmd_skipped(a):
    from pyiceberg.expressions import GreaterThanOrEqual

    since = datetime.fromisoformat(a.since.replace("Z", "+00:00")).astimezone(timezone.utc)
    rows = (catalog(a).load_table("lakehouse.events_skipped")
            .scan(row_filter=GreaterThanOrEqual("kafka_timestamp", since.isoformat()),
                  selected_fields=("kafka_partition", "kafka_offset", "reason", "event_id", "error", "payload"))
            .to_arrow().to_pylist())
    out = {"undecodable": 0, "duplicate": 0, "undecodable_records": []}
    for r in rows:
        out[r["reason"]] = out.get(r["reason"], 0) + 1
        if r["reason"] == "undecodable":
            out["undecodable_records"].append({
                "partition": r["kafka_partition"], "offset": r["kafka_offset"], "error": r["error"],
                "payload": (r["payload"] or b"").decode("utf-8", "replace")})
    print(json.dumps(out))


def cmd_lineage(a):
    expected = {event_id(i) for i in range(a.start, a.start + a.count)}
    rows = [r for r in catalog(a).load_table("lakehouse.events")
            .scan(selected_fields=("event_id", "kafka_partition", "kafka_offset", "kafka_timestamp"))
            .to_arrow().to_pylist() if r["event_id"] in expected]
    with_lineage = [r for r in rows if r["kafka_offset"] is not None]
    print(json.dumps({"rows": len(rows), "with_lineage": len(with_lineage),
                      "partitions": sorted({r["kafka_partition"] for r in with_lineage}),
                      "distinct_offsets": len({(r["kafka_partition"], r["kafka_offset"]) for r in with_lineage})}))


def cmd_delete_one(a):
    import pyarrow.parquet as pq
    from pyiceberg.expressions import EqualTo

    target = a.event_id or event_id(a.event_index)
    cat = catalog(a)
    row = cat.load_table("lakehouse.events").scan(row_filter=EqualTo("event_id", target)).to_arrow()
    if row.num_rows != 1:
        sys.exit(f"expected one row for {target}, found {row.num_rows}")
    pq.write_table(row, a.save)
    with_retries(lambda: cat.load_table("lakehouse.events").delete(delete_filter=EqualTo("event_id", target)))
    r = row.to_pylist()[0]
    print(json.dumps({"event_id": target, "partition": r["kafka_partition"], "offset": r["kafka_offset"],
                      "saved": a.save}))


def cmd_restore(a):
    import pyarrow.parquet as pq

    row = pq.read_table(a.file)
    cat = catalog(a)
    with_retries(lambda: cat.load_table("lakehouse.events").append(row))
    print(json.dumps({"restored": row.num_rows}))


def cmd_old_row(a):
    """The newest row with lineage written between 6 hours and N minutes ago that is not itself a replay."""
    from datetime import timedelta
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=a.older_than_minutes)
    rows = (catalog(a).load_table("lakehouse.events")
            .scan(row_filter=And(LessThan("kafka_timestamp", cutoff.isoformat()),
                                 GreaterThanOrEqual("kafka_timestamp", (cutoff - timedelta(hours=6)).isoformat())),
                  selected_fields=("event_id", "kafka_partition", "kafka_offset", "kafka_timestamp", "attributes"))
            .to_arrow().to_pylist())
    rows = [r for r in rows if REPLAY_MARKER not in dict(r["attributes"] or [])]
    if not rows:
        print(json.dumps({"found": False}))
        return
    r = max(rows, key=lambda x: x["kafka_timestamp"])
    print(json.dumps({"found": True, "event_id": r["event_id"], "partition": r["kafka_partition"],
                      "offset": r["kafka_offset"], "kafka_timestamp": r["kafka_timestamp"].isoformat()}))


def cmd_event(a):
    from pyiceberg.expressions import EqualTo

    rows = (catalog(a).load_table("lakehouse.events")
            .scan(row_filter=EqualTo("event_id", a.event_id),
                  selected_fields=("kafka_partition", "kafka_offset", "attributes")).to_arrow().to_pylist())
    print(json.dumps({"rows": len(rows), "copies": [
        {"partition": r["kafka_partition"], "offset": r["kafka_offset"],
         "replayed_from": dict(r["attributes"] or []).get(REPLAY_MARKER)} for r in rows]}))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog-uri", default="http://localhost:8181")
    p.add_argument("--s3-endpoint", default="http://localhost:8333")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("skipped")
    s.add_argument("--since", required=True)
    s = sub.add_parser("lineage")
    s.add_argument("--start", type=int, required=True)
    s.add_argument("--count", type=int, required=True)
    s = sub.add_parser("delete-one")
    s.add_argument("--event-index", type=int)
    s.add_argument("--event-id")
    s.add_argument("--save", required=True)
    s = sub.add_parser("restore")
    s.add_argument("--file", required=True)
    s = sub.add_parser("old-row")
    s.add_argument("--older-than-minutes", type=float, required=True)
    s = sub.add_parser("event")
    s.add_argument("--event-id", required=True)
    a = p.parse_args()
    {"skipped": cmd_skipped, "lineage": cmd_lineage, "delete-one": cmd_delete_one, "restore": cmd_restore,
     "old-row": cmd_old_row, "event": cmd_event}[a.cmd](a)


if __name__ == "__main__":
    main()
