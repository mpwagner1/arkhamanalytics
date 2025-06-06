from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict, Tuple
import json
import os


def get_schema_dict(df: DataFrame) -> Dict[str, str]:
    """
    Returns a dictionary of column names and their Spark data types
    (as strings).
    """
    return {field.name: field.dataType.simpleString() for field in df.schema}


def infer_schema(df: DataFrame) -> StructType:
    """Return the inferred schema of a DataFrame."""
    return df.schema


def validate_schema(df: DataFrame, expected_schema_path: str) -> dict:
    """
    Validate a Spark DataFrame against a schema defined in a JSON file.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        expected_schema_path (str): Path to a JSON file containing the
            expected schema (e.g., ["column1", "column2", ...])

    Returns:
        dict: {
            "valid": True/False,
            "missing_columns": [...],
            "unexpected_columns": [...],
            "errors": [...],
        }
    """
    result = {
        "valid": False,
        "missing_columns": [],
        "unexpected_columns": [],
        "errors": [],
    }

    if not os.path.exists(expected_schema_path):
        result["errors"].append(
            f"Schema file not found: {expected_schema_path}"
        )
        return result

    try:
        with open(expected_schema_path, "r") as f:
            expected_columns = json.load(f)

        if not isinstance(expected_columns, list):
            result["errors"].append(
                "Expected schema should be a list of column names."
            )
            return result

        actual_columns = df.columns
        missing = [col for col in expected_columns if col not in actual_columns]
        unexpected = [col for col in actual_columns if col not in expected_columns]

        result["missing_columns"] = missing
        result["unexpected_columns"] = unexpected
        result["valid"] = not missing and not unexpected

    except Exception as e:
        result["errors"].append(str(e))

    return result


def validate_schema_from_json(df: DataFrame, path: str) -> Dict:
    with open(path, "r") as f:
        schema_data = json.load(f)

    results = {}

    if "columns" in schema_data:
        is_valid, missing, unexpected = validate_column_names(
            df, schema_data["columns"]
        )
        results["column_name_check"] = {
            "valid": is_valid,
            "missing": missing,
            "unexpected": unexpected,
        }

    if "types" in schema_data:
        is_valid, mismatched, missing = validate_column_types(
            df, schema_data["types"]
        )
        results["type_check"] = {
            "valid": is_valid,
            "mismatched": mismatched,
            "missing": missing,
        }

    results["overall_valid"] = all(
        section["valid"] for section in results.values()
    )
    return results


def get_column_names(df: DataFrame) -> List[str]:
    """Return the list of column names in a DataFrame."""
    return df.columns


def validate_column_names(
    df: DataFrame, expected_columns: List[str], case_sensitive: bool = False
) -> Tuple[bool, List[str], List[str]]:
    """
    Validates if DataFrame contains exactly the expected column names.
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
    df: DataFrame,
    expected_schema: Dict[str, str],
    case_sensitive: bool = False,
) -> Tuple[bool, Dict[str, str], Dict[str, str]]:
    """
    Validates if each column in the DataFrame matches the expected type.
    """
    actual_schema = get_schema_dict(df)
    if not case_sensitive:
        expected_schema = {
            k.lower(): v for k, v in expected_schema.items()
        }
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
