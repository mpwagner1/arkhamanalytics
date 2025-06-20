import chardet
import logging
from os.path import exists
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from glob import glob


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def detect_file_encoding(file_path: str, sample_size: int = 100000) -> str:
    """Detect file encoding using chardet for local paths only."""
    if file_path.startswith("dbfs:/"):
        logger.info(f"Skipping encoding detection for Spark path: {file_path}")
        return "utf-8"  # default for Spark-safe reads

    local_path = file_path.replace("dbfs:/", "/dbfs/") if "dbfs:/" in file_path else file_path

    if not exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    with open(local_path, "rb") as f:
        rawdata = f.read(sample_size)

    result = chardet.detect(rawdata)
    encoding = result["encoding"] or "utf-8"
    logger.info(f"Detected encoding for {file_path}: {encoding}")
    return encoding

def read_file_as_df(
    spark: SparkSession,
    file_path: str,
    file_format: str,
    encoding: Optional[str] = None,
    sheet_name: Optional[str] = None,
    start_cell: Optional[str] = None,
) -> DataFrame:
    """Read file into a PySpark DataFrame based on format and encoding."""

    logger.info(f"Attempting to read: {file_path}")
    logger.info(f"Format: {file_format}")
    logger.info(f"Encoding: {encoding}")

    # Skip existence check for Spark paths
    if not file_path.startswith("dbfs:/"):
        if not exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    # Only detect encoding for local files
    if encoding is None and not file_path.startswith("dbfs:/"):
        encoding = detect_file_encoding(file_path)

def read_file_as_df(
    spark: SparkSession,
    file_path: str,
    file_format: str,
    encoding: Optional[str] = None,
    sheet_name: Optional[str] = None,
    start_cell: Optional[str] = None,
) -> DataFrame:
    """Read file into a PySpark DataFrame based on format and encoding."""

    logger.info(f"Attempting to read: {file_path}")
    logger.info(f"Format: {file_format}")
    logger.info(f"Encoding: {encoding}")

    try:
        # Skip existence check for Spark paths
        if not file_path.startswith("dbfs:/"):
            if not exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

        # Only detect encoding for local files
        if encoding is None and not file_path.startswith("dbfs:/"):
            encoding = detect_file_encoding(file_path)

        if file_format == "csv":
            return (
                spark.read.option("header", "true")
                .option("encoding", encoding or "utf-8")
                .csv(file_path)
            )

        elif file_format == "txt":
            return (
                spark.read.option("header", "true")
                .option("encoding", encoding or "utf-8")
                .csv(file_path, sep="\t")
            )

        elif file_format.lower() == "xlsx":
            data_address = f"'{sheet_name}'!{start_cell}"
            reader = (
                spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dataAddress", data_address)
                .option("sheetName", sheet_name)
                .option("encoding", encoding)
                .load(file_path)
            )
            return reader

        # Final fallback
        raise ValueError(f"Unsupported file format: {file_format}")

    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        raise RuntimeError(f"Failed to read file {file_path}: {str(e)}")


def detect_and_read_file(
    spark: SparkSession,
    file_path: str,
    encoding: Optional[str] = None,
    sheet_name: Optional[str] = None,
    start_cell: Optional[str] = None,
) -> DataFrame:
    """Convenience wrapper to detect file format and read the file."""
    try:
        file_format = get_file_extension(file_path)
        return read_file_as_df(
            spark,
            file_path,
            file_format,
            encoding,
            sheet_name,
            start_cell,
        )
    except Exception as e:
        logger.error(f"Error in detect_and_read_file: {str(e)}")
        raise RuntimeError(f"Failed to detect/read file {file_path}: {str(e)}")

def resolve_file_path_for_spark(container_name: str, file_pattern: str) -> str:
    """
    Resolves a path using /dbfs/mnt for globbing,
    then returns a Spark-compatible path (dbfs:/mnt/...)
    """
    search_path = f"/dbfs/mnt/{container_name}/{file_pattern}"
    matched_files = glob(search_path)
    if not matched_files:
        raise FileNotFoundError(f"No file found for pattern: {search_path}")
    
    # Convert /dbfs/mnt/... to dbfs:/mnt/...
    path = Path(matched_files[0])
    dbfs_path = str(path).replace("/dbfs", "dbfs:")
    return dbfs_path

def get_file_extension(file_path: str) -> str:
    """Returns the lowercase file extension (e.g., 'csv', 'xlsx', 'txt')."""
    suffix = Path(file_path).suffix
    return suffix.lower().replace(".", "") if suffix else ""

