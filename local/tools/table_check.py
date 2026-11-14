#!/usr/bin/env python3
"""Independent check of the events table through PyIceberg (a different reader than Flink or Spark).

  table_check.py [--expect-rows N]      prints rows, distinct event ids, files, snapshots; exits 1 on mismatch.

Needs `kubectl port-forward` for the catalog (8181) and S3 (8333), like eo_test.py.
"""
import argparse
import json
import sys

from pyiceberg.catalog import load_catalog


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--expect-rows", type=int)
    p.add_argument("--table", default="lakehouse.events")
    p.add_argument("--catalog-uri", default="http://localhost:8181")
    p.add_argument("--s3-endpoint", default="http://localhost:8333")
    a = p.parse_args()
    catalog = load_catalog("lakehouse", **{
        "type": "rest", "uri": a.catalog_uri, "s3.endpoint": a.s3_endpoint,
        "s3.access-key-id": "lakehouse-local", "s3.secret-access-key": "lakehouse-local-secret",
        "s3.path-style-access": "true", "s3.region": "us-east-1"})
    table = catalog.load_table(a.table)
    ids = table.scan(selected_fields=("event_id",)).to_arrow().column("event_id")
    # inspect.files() raises on a table with no data files (e.g. right after a reset), so count only when rows exist.
    result = {"rows": len(ids), "distinct_ids": len(ids.unique()),
              "data_files": len(table.inspect.files()) if len(ids) else 0,
              "snapshots": len(table.snapshots())}
    print(json.dumps(result))
    ok = result["rows"] == result["distinct_ids"] and (a.expect_rows is None or result["rows"] == a.expect_rows)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
