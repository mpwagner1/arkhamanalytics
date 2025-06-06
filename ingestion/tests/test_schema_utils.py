import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from ingestion.modules import schema_utils


def test_get_column_names(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    assert schema_utils.get_column_names(df) == ["id", "name"]


def test_get_schema_dict(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    result = schema_utils.get_schema_dict(df)
    assert result == {"id": "bigint", "name": "string"}


def test_validate_column_names_exact_match(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    valid, missing, unexpected = schema_utils.validate_column_names(df, ["id", "name"])
    assert valid is True
    assert missing == []
    assert unexpected == []


def test_validate_column_names_missing_column(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    valid, missing, unexpected = schema_utils.validate_column_names(df, ["id", "name"])
    assert valid is False
    assert "name" in missing


def test_validate_column_types_match(spark):
    schema = StructType(
        [StructField("id", IntegerType()), StructField("name", StringType())]
    )
    df = spark.createDataFrame([(1, "a")], schema)
    expected_schema = {"id": "int", "name": "string"}
    valid, mismatched, missing = schema_utils.validate_column_types(
        df, expected_schema
    )
    assert valid
    assert not mismatched
    assert not missing


def test_compare_schemas_mismatch(spark):
    df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
    df2 = spark.createDataFrame([(1,)], ["id"])
    result = schema_utils.compare_schemas(df1, df2)
    assert "name" in result["only_in_df1"]
    assert not result["only_in_df2"]
