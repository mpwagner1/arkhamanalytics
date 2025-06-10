import chardet
import logging
from os.path import exists
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def detect_file_encoding(file_path: str, sample_size: int = 100000) -> str:
    """Detect file encoding using chardet."""
    if not exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "rb") as f:
        rawdata = f.read(sample_size)

    result = chardet.detect(rawdata)
    encoding = result["encoding"] or "utf-8"
    logger.info(f"Detected encoding for {file_path}: {encoding}")
    return encoding


def get_file_extension(file_path: str) -> str:
    """Returns the lowercase file extension (e.g., 'csv', 'xlsx', 'txt')."""
    suffix = Path(file_path).suffix
    return suffix.lower().replace(".", "") if suffix else ""


def read_file_as_df(
    spark: SparkSession,
    file_path: str,
    file_format: str,
    encoding: Optional[str] = None,
    sheet_name: Optional[str] = None,
    start_cell: Optional[str] = None,
) -> DataFrame:
    """Read file into a PySpark DataFrame based on format and encoding."""
    if not exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if encoding is None:
        encoding = detect_file_encoding(file_path)

    try:
        if file_format == "csv":
            logger.info(f"Reading CSV file: {file_path}")
            return (
                spark.read.option("header", "true")
                .option("encoding", encoding)
                .csv(file_path)
            )

        elif file_format == "txt":
            logger.info(f"Reading TXT file: {file_path}")
            return (
                spark.read.option("header", "true")
                .option("encoding", encoding)
                .csv(file_path, sep="\t")
            )

        elif file_format in ["xls", "xlsx"]:
            logger.info(f"Reading Excel file: {file_path}")
    
            # Default to "A1" if nothing is provided
            sheet = sheet_name or "Sheet1"
            cell = start_cell or "A1"
            data_address = f"'{sheet}'!{cell}"
    
            return (
                spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dataAddress", data_address)
                .option("maxRowsInMemory", 1000)
                .load(file_path)
            )

        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        raise RuntimeError(f"Failed to read file {file_path}: {str(e)}")


def detect_and_read_file(
    spark: SparkSession,
    file_path: str,
    encoding: Optional[str] = None,
) -> DataFrame:
    """Convenience wrapper to detect file format and read the file."""
    try:
        file_format = get_file_extension(file_path)
        return read_file_as_df(spark, file_path, file_format, encoding)
    except Exception as e:
        logger.error(f"Error in detect_and_read_file: {str(e)}")
        raise RuntimeError(f"Failed to detect/read file {file_path}: {str(e)}")
