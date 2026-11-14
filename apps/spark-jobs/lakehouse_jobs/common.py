"""Session and catalog configuration shared by the jobs."""
import json
import os
from datetime import datetime, timezone
from typing import Mapping, Optional

from pyspark.sql import SparkSession

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
DEFAULT_CATALOG = "lakehouse"


def catalog_conf(env: Optional[Mapping[str, str]] = None, catalog: str = DEFAULT_CATALOG) -> dict:
    """Spark settings for the Iceberg catalog (REST locally, Glue on AWS), read from environment variables."""
    env = os.environ if env is None else env
    catalog_type = env.get("ICEBERG_CATALOG_TYPE", "rest")
    if catalog_type not in ("rest", "glue"):
        raise ValueError(f"ICEBERG_CATALOG_TYPE must be rest or glue, got {catalog_type!r}")
    prefix = f"spark.sql.catalog.{catalog}"
    conf = {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        "spark.sql.session.timeZone": "UTC",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": catalog_type,
        f"{prefix}.warehouse": env.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.client.region": env.get("AWS_REGION", "us-east-1"),
    }
    if catalog_type == "rest":
        conf[f"{prefix}.uri"] = env.get("ICEBERG_REST_URI", "http://iceberg-rest.data.svc:8181")
    if env.get("S3_ENDPOINT"):
        conf[f"{prefix}.s3.endpoint"] = env["S3_ENDPOINT"]
        conf[f"{prefix}.s3.path-style-access"] = "true"
    if env.get("S3_ACCESS_KEY_ID"):
        conf[f"{prefix}.s3.access-key-id"] = env["S3_ACCESS_KEY_ID"]
        conf[f"{prefix}.s3.secret-access-key"] = env["S3_SECRET_ACCESS_KEY"]
    return conf


def build_session(app_name: str) -> SparkSession:
    """Creates the session for a job. Settings given by the SparkApplication (executors, AQE...) win."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in catalog_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def catalog_of(table: str) -> str:
    """The catalog part of a fully qualified table name (catalog.namespace.table)."""
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"expected catalog.namespace.table, got {table!r}")
    return parts[0]


def procedure_table(table: str) -> str:
    """Fully qualified table name: `lakehouse.events` is ambiguous when the namespace has the catalog's name."""
    catalog_of(table)  # validates catalog.namespace.table
    return table


def utc_timestamp_literal(moment: datetime) -> str:
    """Formats a moment as a Spark SQL timestamp literal in UTC (the session time zone)."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def emit_summary(job: str, payload: dict) -> None:
    """One machine-readable line per run: `SUMMARY {...}`. Easy to grep from driver logs."""
    print("SUMMARY " + json.dumps({"job": job, **payload}, default=str), flush=True)
