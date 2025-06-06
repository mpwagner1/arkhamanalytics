import pytest
from pyspark.sql import SparkSession
from ingestion.modules.audit_logger import log_ingestion_audit
import os
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder \
        .master("local[1]") \
        .appName("AuditLoggerTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()

def test_log_ingestion_audit_success(tmp_path, spark):
    audit_path = str(tmp_path / "audit_log")

    log_ingestion_audit(
        spark=spark,
        audit_target=audit_path,
        source_file_path="/mnt/raw/test.csv",
        file_format="csv",
        row_count=123,
        source_schema="StructType(...)",
        source_system="TestSystem",
        triggered_by="test_user",
        status="SUCCESS",
        target_type="path"
    )

    result_df = spark.read.format("delta").load(audit_path)
    data = result_df.collect()

    assert result_df.columns == [
        "timestamp_utc",
        "source_file_path",
        "file_format",
        "row_count",
        "source_schema",
        "status",
        "error_message",
        "source_system",
        "triggered_by"
    ]
    assert len(data) == 1
    assert data[0]["row_count"] == 123
    assert data[0]["status"] == "SUCCESS"
    assert data[0]["source_file_path"] == "/mnt/raw/test.csv"

def test_log_ingestion_audit_invalid_target_type_raises(spark):
    with pytest.raises(ValueError, match="target_type must be 'path' or 'table'"):
        log_ingestion_audit(
            spark=spark,
            audit_target="some_target",
            source_file_path="/mnt/raw/test.csv",
            file_format="csv",
            row_count=1,
            source_schema="x",
            target_type="bad_type"
        )
