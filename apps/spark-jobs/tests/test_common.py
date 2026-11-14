from datetime import datetime, timedelta, timezone

import pytest

from lakehouse_jobs import common


def test_local_catalog_conf_uses_endpoint_path_style_and_static_keys():
    conf = common.catalog_conf({
        "ICEBERG_REST_URI": "http://rest:8181", "S3_ENDPOINT": "http://s3:8333",
        "S3_ACCESS_KEY_ID": "id", "S3_SECRET_ACCESS_KEY": "secret"})

    assert conf["spark.sql.catalog.lakehouse.uri"] == "http://rest:8181"
    assert conf["spark.sql.catalog.lakehouse.s3.endpoint"] == "http://s3:8333"
    assert conf["spark.sql.catalog.lakehouse.s3.path-style-access"] == "true"
    assert conf["spark.sql.catalog.lakehouse.s3.access-key-id"] == "id"
    assert conf["spark.sql.session.timeZone"] == "UTC"


def test_aws_style_conf_leaves_credentials_to_the_default_chain():
    conf = common.catalog_conf({})

    assert not any("s3.endpoint" in k or "access-key" in k or "path-style" in k for k in conf)
    assert conf["spark.sql.catalog.lakehouse.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"


def test_table_name_helpers():
    assert common.catalog_of("lakehouse.lakehouse.events") == "lakehouse"
    # stored procedures get the full three-part name (a namespace named like the catalog is ambiguous)
    assert common.procedure_table("lakehouse.lakehouse.events") == "lakehouse.lakehouse.events"
    with pytest.raises(ValueError):
        common.catalog_of("events")


def test_timestamp_literals_are_utc_regardless_of_input_zone():
    plus_two = timezone(timedelta(hours=2))
    assert common.utc_timestamp_literal(datetime(2026, 10, 28, 12, 0, tzinfo=plus_two)) == "2026-10-28 10:00:00"


def test_summary_is_one_greppable_json_line(capsys):
    common.emit_summary("x", {"a": 1})
    assert capsys.readouterr().out.strip() == 'SUMMARY {"job": "x", "a": 1}'


def test_glue_catalog_conf_has_no_rest_uri_and_no_static_keys():
    conf = common.catalog_conf({"ICEBERG_CATALOG_TYPE": "glue", "ICEBERG_WAREHOUSE": "s3://bucket/iceberg"})

    assert conf["spark.sql.catalog.lakehouse.type"] == "glue"
    assert conf["spark.sql.catalog.lakehouse.warehouse"] == "s3://bucket/iceberg"
    assert "spark.sql.catalog.lakehouse.uri" not in conf
    assert not any("access-key" in k or "s3.endpoint" in k for k in conf)


def test_unknown_catalog_type_is_rejected():
    with pytest.raises(ValueError, match="rest or glue"):
        common.catalog_conf({"ICEBERG_CATALOG_TYPE": "hive"})
