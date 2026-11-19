#!/usr/bin/env python3
"""Exactly-once test helper for the Kafka -> Flink -> Iceberg pipeline; event i has event_id uuid5(NAMESPACE_URL, "eo-<i>").

  eo_test.py produce --start 0 --count 8000 [--dup-every 10] [--url http://localhost:8082/v1/events]
  eo_test.py resend  --start 0 --count 1000
  eo_test.py verify-evolution --total 1500 --with-region 500 --region eu-west-1
  eo_test.py verify  --expected 24000 [--catalog-uri http://localhost:8181 --s3-endpoint http://localhost:8333]
  eo_test.py stream --start 8000000 --rate 200 --seconds 600     (steady load for chaos experiments)
             [--drop-reply-every 20]                            (every 20th batch: the reply is thrown away unread)
  eo_test.py verify-range --start 8000000 --count 12000
"""
import argparse
import http.client
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

BATCH = 200
REGION = None  # set by --region; added to every event when given


def event_id(i):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"eo-{i}"))


def make_event(i):
    event = {"event_id": event_id(i), "source": f"eo-src-{i % 20}", "type": "eo.test", "value": float(i),
             "attributes": {"n": str(i)}}
    if REGION:
        event["region"] = REGION
    return event


def post(url, events):
    body = json.dumps(events).encode()
    for attempt in range(1, 31):
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                time.sleep(min(2.0, 0.2 * attempt))
                continue
            sys.exit(f"HTTP {e.code}: {e.read().decode()[:300]}")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(min(2.0, 0.2 * attempt))
    sys.exit("giving up after repeated failures")


def send(url, ids):
    sent = 0
    for k in range(0, len(ids), BATCH):
        post(url, [make_event(i) for i in ids[k:k + BATCH]])
        sent += len(ids[k:k + BATCH])
    return sent


def cmd_produce(a):
    global REGION
    REGION = a.region
    ids = list(range(a.start, a.start + a.count))
    send(a.url, ids)
    dups = [i for i in ids if a.dup_every and i % a.dup_every == 0]
    send(a.url, dups)  # the "client retries": same event_id sent again later
    print(f"produced {len(ids)} events ({a.start}..{a.start + a.count - 1}) + {len(dups)} duplicate sends")


def cmd_resend(a):
    ids = list(range(a.start, a.start + a.count))
    send(a.url, ids)
    print(f"re-sent {len(ids)} already delivered events ({a.start}..{a.start + a.count - 1})")


def cmd_verify(a):
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("lakehouse", **{
        "type": "rest", "uri": a.catalog_uri,
        "s3.endpoint": a.s3_endpoint, "s3.access-key-id": "lakehouse-local",
        "s3.secret-access-key": "lakehouse-local-secret", "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    })
    table = catalog.load_table("lakehouse.events")
    rows = table.scan(selected_fields=("event_id", "attributes")).to_arrow().to_pylist()
    ids = [r["event_id"] for r in rows]
    distinct = set(ids)
    expected = {event_id(i) for i in range(a.expected)}
    missing = expected - distinct
    unexpected = distinct - expected
    duplicates = len(ids) - len(distinct)
    snapshots = len(table.snapshots())
    files = len(table.inspect.files()) if hasattr(table, "inspect") else -1
    print(json.dumps({"rows": len(ids), "distinct_ids": len(distinct), "expected": a.expected,
                      "missing": len(missing), "unexpected": len(unexpected), "duplicates": duplicates,
                      "snapshots": snapshots, "data_files": files}))
    if missing or unexpected or duplicates:
        sys.exit(1)


def send_and_drop_reply(url, body):
    """Send a batch and close the connection without reading the reply: the server has the events, the client never
    learns the outcome. This is the "Kafka accepted it but the acknowledgement was lost" case."""
    u = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=15)
    try:
        conn.request("POST", u.path, body=body, headers={"Content-Type": "application/json"})
        time.sleep(0.5)  # give the request time to cross the port-forward before the connection goes away
    except OSError:
        pass  # the request may not have arrived either; the retry below covers both cases
    finally:
        conn.close()


def cmd_stream(a):
    """Steady load for chaos experiments: `rate` events/s for `seconds`; a batch failing past `give_up` is reported uncertain."""
    batch, give_up = 50, 180
    deadline = time.monotonic() + a.seconds
    next_id, retried, dropped, batches = a.start, 0, 0, 0
    while time.monotonic() < deadline:
        started = time.monotonic()
        body = json.dumps([make_event(i) for i in range(next_id, next_id + batch)]).encode()
        batches += 1
        if a.drop_reply_every and batches % a.drop_reply_every == 0:
            send_and_drop_reply(a.url, body)
            dropped += 1  # then retried below with the same ids, as a real client would
        while True:
            try:
                req = urllib.request.Request(a.url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=15):
                    break
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ConnectionError):
                retried += 1
                if time.monotonic() - started > give_up:
                    print(json.dumps({"start": a.start, "sent": next_id - a.start, "uncertain": [next_id, next_id + batch],
                                      "retried": retried, "dropped_replies": dropped, "complete": False}))
                    sys.exit(2)
                time.sleep(1)
        next_id += batch
        time.sleep(max(0.0, batch / a.rate - (time.monotonic() - started)))
    print(json.dumps({"start": a.start, "sent": next_id - a.start, "retried": retried, "dropped_replies": dropped,
                      "batch": batch, "complete": True}))


def cmd_verify_range(a):
    """Every id in start .. start+count-1 is in the table exactly once (rows from other runs are ignored)."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("lakehouse", **{
        "type": "rest", "uri": a.catalog_uri,
        "s3.endpoint": a.s3_endpoint, "s3.access-key-id": "lakehouse-local",
        "s3.secret-access-key": "lakehouse-local-secret", "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    })
    expected = {event_id(i) for i in range(a.start, a.start + a.count)}
    ids = [r["event_id"] for r in catalog.load_table("lakehouse.events")
           .scan(selected_fields=("event_id",)).to_arrow().to_pylist() if r["event_id"] in expected]
    found = set(ids)
    result = {"expected": len(expected), "found": len(found), "missing": len(expected - found),
              "duplicates": len(ids) - len(found)}
    print(json.dumps(result))
    if result["missing"] or result["duplicates"]:
        sys.exit(1)


def cmd_verify_evolution(a):
    """After the demo: schema has the new column, old rows read back with null, new rows carry the value."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("lakehouse", **{
        "type": "rest", "uri": a.catalog_uri,
        "s3.endpoint": a.s3_endpoint, "s3.access-key-id": "lakehouse-local",
        "s3.secret-access-key": "lakehouse-local-secret", "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    })
    table = catalog.load_table("lakehouse.events")
    columns = [f.name for f in table.schema().fields]
    rows = table.scan(selected_fields=("event_id", "region")).to_arrow().to_pylist()
    with_region = sum(1 for r in rows if r["region"] == a.region)
    null_region = sum(1 for r in rows if r["region"] is None)
    ids = [r["event_id"] for r in rows]
    oldest = min(table.snapshots(), key=lambda snap: snap.timestamp_ms)
    old_scan = table.scan(snapshot_id=oldest.snapshot_id, selected_fields=("event_id",)).to_arrow()
    result = {"schema_columns": columns, "schema_versions": len(table.metadata.schemas), "rows": len(rows),
              "distinct_ids": len(set(ids)), "with_region": with_region, "null_region": null_region,
              "oldest_snapshot_rows": old_scan.num_rows}
    print(json.dumps(result))
    ok = ("region" in columns and len(table.metadata.schemas) >= 2 and len(rows) == a.total
          and len(set(ids)) == a.total and with_region == a.with_region and null_region == a.total - a.with_region)
    if not ok:
        sys.exit(1)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, fn in (("produce", cmd_produce), ("resend", cmd_resend)):
        s = sub.add_parser(name)
        s.add_argument("--start", type=int, required=True)
        s.add_argument("--count", type=int, required=True)
        s.add_argument("--url", default="http://localhost:8082/v1/events")
        if name == "produce":
            s.add_argument("--dup-every", type=int, default=0)
            s.add_argument("--region", default=None, help="set the optional region field (schema v2)")
        s.set_defaults(fn=fn)
    v = sub.add_parser("verify")
    v.add_argument("--expected", type=int, required=True)
    v.add_argument("--catalog-uri", default="http://localhost:8181")
    v.add_argument("--s3-endpoint", default="http://localhost:8333")
    v.set_defaults(fn=cmd_verify)
    st = sub.add_parser("stream")
    st.add_argument("--start", type=int, required=True)
    st.add_argument("--rate", type=float, default=200, help="events per second")
    st.add_argument("--seconds", type=float, required=True)
    st.add_argument("--url", default="http://localhost:8082/v1/events")
    st.add_argument("--drop-reply-every", type=int, default=0, help="every Nth batch: send, discard the reply, retry")
    st.set_defaults(fn=cmd_stream)
    vr = sub.add_parser("verify-range")
    vr.add_argument("--start", type=int, required=True)
    vr.add_argument("--count", type=int, required=True)
    vr.add_argument("--catalog-uri", default="http://localhost:8181")
    vr.add_argument("--s3-endpoint", default="http://localhost:8333")
    vr.set_defaults(fn=cmd_verify_range)
    e = sub.add_parser("verify-evolution")
    e.add_argument("--total", type=int, required=True)
    e.add_argument("--with-region", type=int, required=True)
    e.add_argument("--region", required=True)
    e.add_argument("--catalog-uri", default="http://localhost:8181")
    e.add_argument("--s3-endpoint", default="http://localhost:8333")
    e.set_defaults(fn=cmd_verify_evolution)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
