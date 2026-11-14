import itertools

import pytest
from pyspark.sql import SparkSession

from lakehouse_jobs.common import ICEBERG_EXTENSIONS


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark with a Hadoop-type Iceberg catalog named `lakehouse` (the name the jobs use)."""
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-jobs-tests")
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.db")
    yield session
    session.stop()


_counter = itertools.count()


@pytest.fixture()
def events_table(spark):
    """A fresh events table laid out like the one the Flink job creates."""
    name = f"lakehouse.db.events_{next(_counter)}"
    spark.sql(f"""
        CREATE TABLE {name} (
          event_id string, source string, type string, event_time timestamp, ingest_time timestamp,
          value double, attributes map<string,string>, region string)
        USING iceberg PARTITIONED BY (days(event_time))
        TBLPROPERTIES ('format-version'='2')""")
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
