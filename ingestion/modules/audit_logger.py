from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define schema explicitly to avoid type inference issues
AUDIT_SCHEMA = StructType([
    StructField("timestamp_utc", StringType(), True),
    StructField("source_file_path", StringType(), True),
    StructField("file_format", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("source_schema", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("triggered_by", StringType(), True),
])


def _get_current_user(spark: SparkSession) -> str:
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        return "unknown_user"


def log_ingestion_audit(
    spark: SparkSession,
    audit_target: str,
    source_file_path: str,
    file_format: str,
    row_count: int,
    source_schema: str,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
    source_system: Optional[str] = None,
    triggered_by: Optional[str] = None,
    write_mode: str = "append",
    target_type: str = "path",  # can be "path" or "table"
) -> None:
    """
    Logs ingestion metadata to a Delta table or path.
    """
    try:
        if target_type not in {"path", "table"}:
            raise ValueError("target_type must be 'path' or 'table'")

        now = datetime.utcnow().isoformat()
        if triggered_by is None:
            triggered_by = _get_current_user(spark)

        data = [
            (
                now,
                source_file_path,
                file_format,
                row_count,
                source_schema,
                status,
                error_message,
                source_system,
                triggered_by,
            )
        ]

        df: DataFrame = spark.createDataFrame(data, schema=AUDIT_SCHEMA)

        writer = df.write.format("delta").mode(write_mode)
        if target_type == "path":
            writer.save(audit_target)
        else:
            writer.saveAsTable(audit_target)

        logger.info(f"Audit log written to {audit_target} ({target_type})")

    except Exception as e:
        logger.error(f"Failed to write audit log: {str(e)}")
        raise
