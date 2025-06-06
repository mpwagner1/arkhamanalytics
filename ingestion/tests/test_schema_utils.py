from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict, Tuple


def get_schema_dict(df: DataFrame) -> Dict[str, str]:
    """Returns a dictionary of column names and their Spark data types (as strings)."""
    return {field.name: field.dataType.simpleString() for field in df.schema}


def infer_schema(df: DataFrame) -> StructType:
    """Return the inferred schema of a DataFrame."""
    return df.schema


def get_column_names(df: DataFrame) -> List[str]:
    """Return the list of column names in a DataFrame."""
    return df.columns


def validate_column_names(
    df: DataFrame, expected_columns: List[str], case_sensitive: bool = False
) -> Tuple[bool, List[str], List[str]]:
    """
    Validates if DataFrame contains exactly the expected column names.

    Returns:
        Tuple (is_valid, missing_columns, unexpected_columns)
    """
    df_columns = df.columns
    if not case_sensitive:
        df_columns = [col.lower() for col in df_columns]
        expected_columns = [col.lower() for col in expected_columns]

    actual_set = set(df_columns)
    expected_set = set(expected_columns)

    missing = list(expected_set - actual_set)
    unexpected = list(actual_set - expected_set)
    is_valid = len(missing) == 0 and len(unexpected) == 0
    return is_valid, missing, unexpected


def validate_column_types(
    df: DataFrame, expected_schema: Dict[str, str], case_sensitive: bool = False
) -> Tuple[bool, Dict[str, str], Dict[str, str]]:
    """
    Validates if each column in the DataFrame matches the expected type.

    Returns:
        Tuple (is_valid, mismatched_columns, missing_columns)
    """
    actual_schema = get_schema_dict(df)
    if not case_sensitive:
        expected_schema = {k.lower(): v for k, v in expected_schema.items()}
        actual_schema = {k.lower(): v for k, v in actual_schema.items()}

    mismatched = {}
    missing = {}

    for col, expected_type in expected_schema.items():
        actual_type = actual_schema.get(col)
        if actual_type is None:
            missing[col] = expected_type
        elif actual_type != expected_type:
            mismatched[col] = (
                f"expected: {expected_type}, found: {actual_type}"
            )

    is_valid = len(mismatched) == 0 and len(missing) == 0
    return is_valid, mismatched, missing


def compare_schemas(
    df1: DataFrame, df2: DataFrame, ignore_case: bool = False
) -> Dict[str, List[str]]:
    """
    Compares schema of two DataFrames.

    Returns:
        Dict containing differences: keys = only_in_df1, only_in_df2, mismatched_types
    """
    schema1 = get_schema_dict(df1)
    schema2 = get_schema_dict(df2)

    if ignore_case:
        schema1 = {k.lower(): v for k, v in schema1.items()}
        schema2 = {k.lower(): v for k, v in schema2.items()}

    only_in_df1 = list(set(schema1.keys()) - set(schema2.keys()))
    only_in_df2 = list(set(schema2.keys()) - set(schema1.keys()))
    mismatched_types = [
        f"{col}: {schema1[col]} != {schema2[col]}"
        for col in set(schema1.keys()) & set(schema2.keys())
        if schema1[col] != schema2[col]
    ]

    return {
        "only_in_df1": sorted(only_in_df1),
        "only_in_df2": sorted(only_in_df2),
        "mismatched_types": sorted(mismatched_types),
    }
