from typing import List, Any, Dict
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col,
    lower,
    regexp_replace,
    lit,
    concat,
    to_date,
    when,
)

def standardize_column_names(df: DataFrame) -> DataFrame:
    """Standardize column names by converting them to lowercase and replacing spaces with underscores."""
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))
    return df


def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """Trim whitespace and convert text column to lowercase."""
    return df.withColumn(column_name, lower(regexp_replace(col(column_name), r"\s+", " ")))


def remove_currency_symbols(df: DataFrame, column_name: str, output_col: str) -> DataFrame:
    """Remove currency symbols and cast to float."""
    return df.withColumn(output_col, regexp_replace(col(column_name), r"[^0-9.]", "").cast("float"))


def fill_nulls(df: DataFrame, column_defaults: Dict[str, Any]) -> DataFrame:
    """Fill nulls in specified columns with default values."""
    return df.fillna(column_defaults)


def normalize_posting_period(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    Normalize a posting period column from 'MM/YYYY' to a full date 'YYYY-MM-DD' (1st of month).
    Spark requires an explicit format for reliable parsing.
    """
    return df.withColumn(
        target_col,
        to_date(concat(lit("01/"), col(source_col)), "dd/MM/yyyy")
    )


def replace_values(df: DataFrame, column: str, replacements: Dict[Any, Any]) -> DataFrame:
    """
    Replace values in a column based on a mapping dictionary.
    """
    replacement_expr: Column = col(column)
    for old_value, new_value in replacements.items():
        replacement_expr = when(col(column) == old_value, new_value).otherwise(replacement_expr)
    return df.withColumn(column, replacement_expr)
