from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, to_date, lit
)
from typing import Any

def standardize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase and strip whitespace from all column names."""
    for col_name in df.columns:
        cleaned = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, cleaned)
    return df


def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """Trim whitespace, collapse multiple spaces, and lowercase."""
    return df.withColumn(
        column_name,
        lower(trim(regexp_replace(col(column_name), r"\s+", " ")))
    )


def remove_currency_symbols(df: DataFrame, column_name: str, output_col: str) -> DataFrame:
    """Remove currency symbols and cast to float."""
    return df.withColumn(output_col, regexp_replace(col(column_name), r"[^0-9.]", "").cast("float"))

def fill_nulls(df: DataFrame, fill_map: dict[str, Any]) -> DataFrame:
    """Replace nulls in given columns with a default value."""
    return df.fillna(fill_map)

def normalize_posting_period(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    return df.withColumn(output_col, to_date(col(input_col), "MM/yyyy"))

def replace_values(df: DataFrame, column: str, replacements: dict) -> DataFrame:
    """
    Replace values in a column based on a dictionary.
    Example: {'N/A': None, 'null': None}
    """
    new_col = col(column)
    for old_value, new_value in replacements.items():
        new_col = when(col(column) == old_value, lit(new_value)).otherwise(new_col)
    return df.withColumn(column, new_col)
