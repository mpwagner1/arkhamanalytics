from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, to_date, lit
)
from typing import List


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase and strip whitespace from all column names."""
    for col_name in df.columns:
        cleaned = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, cleaned)
    return df


def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """Strip leading/trailing spaces and convert to lowercase."""
    return df.withColumn(
        column_name,
        lower(trim(col(column_name)))
    )


def remove_currency_symbols(df: DataFrame, column_name: str) -> DataFrame:
    """
    Convert currency-formatted strings to float:
    "$1,234.56" → 1234.56
    """
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[$,]", "").cast("double")
    )


def fill_nulls(df: DataFrame, columns: List[str], fill_value) -> DataFrame:
    """Replace nulls in given columns with a default value."""
    return df.fillna({col: fill_value for col in columns})


def normalize_posting_period(df: DataFrame, column_name: str, new_column_name: str = "posting_date") -> DataFrame:
    """
    Transform 'MM/YYYY' string column into a first-of-month date column.
    Example: '05/2022' → '2022-05-01'
    """
    return df.withColumn(
        new_column_name,
        to_date(
            regexp_replace(col(column_name), r"^(\d{2})/(\d{4})$", r"\2-\1-01"),
            "yyyy-MM-dd"
        )
    )


def replace_values(df: DataFrame, column: str, replacements: dict) -> DataFrame:
    """
    Replace values in a column based on a dictionary.
    Example: {'N/A': None, 'null': None}
    """
    new_col = col(column)
    for old_value, new_value in replacements.items():
        new_col = when(col(column) == old_value, lit(new_value)).otherwise(new_col)
    return df.withColumn(column, new_col)
