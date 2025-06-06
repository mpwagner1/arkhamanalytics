from datetime import datetime
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AUDIT_COLUMNS = [
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

def _get_current_user(spark: SparkSession) -> str:
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except:
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
    target_type: str = "path"  # can be "path" or "table"
) -> None:
    """
    Logs ingestion metadata to a Delta table or path.
    
    Args:
        spark: Active SparkSession
        audit_target: Path or table name for audit logs
        source_file_path: Ingested file path
        file_format: csv, xlsx, txt, etc.
        row_count: Number of records ingested
        source_schema: Stringified schema
        status: SUCCESS or FAILED
        error_message: Optional error message
        source_system: Optional data source name
        triggered_by: Optional user/job, auto-filled if None
        write_mode: append, overwrite, etc.
        target_type: 'path' or 'table'
    """

    try:
        now = datetime.utcnow().isoformat()
        if triggered_by is None:
            triggered_by = _get_current_user(spark)

        data = [(now, source_file_path, file_format, row_count, source_schema,
                 status, error_message, source_system, triggered_by)]

        df: DataFrame = spark.createDataFrame(data, AUDIT_COLUMNS)

        writer = df.write.format("delta").mode(write_mode)
        if target_type == "path":
            writer.save(audit_target)
        elif target_type == "table":
            writer.saveAsTable(audit_target)
        else:
            raise ValueError("target_type must be 'path' or 'table'")

        logger.info(f"Audit log written to {audit_target} ({target_type})")

    except Exception as e:
        logger.error(f"Failed to write audit log: {str(e)}")
        # Optionally re-raise to stop pipeline or continue silently
        raise
