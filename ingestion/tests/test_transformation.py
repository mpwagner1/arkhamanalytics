import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import arkhamanalytics.modules.transformations as transformations

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("TestSession").getOrCreate()


def test_standardize_column_names(spark):
    df = spark.createDataFrame([(1, 2)], ["Col One", "Col Two"])
    result = transformations.standardize_column_names(df)
    assert set(result.columns) == {"col_one", "col_two"}


def test_clean_text_column_removes_whitespace_and_lowercases(spark):
    df = spark.createDataFrame([("  Hello   World  ",)], ["text"])
    result = transformations.clean_text_column(df, "text")
    assert result.first()["text"] == "hello world"


def test_remove_currency_symbols_parses_float(spark):
    df = spark.createDataFrame([("$1,234.56",)], ["amount"])
    result = transformations.remove_currency_symbols(df, "amount", "clean_amount")
    assert result.first()["clean_amount"] == pytest.approx(1234.56)


def test_fill_nulls_applies_correctly(spark):
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", IntegerType(), True),
    ])
    df = spark.createDataFrame([(None, 2)], schema)
    result = transformations.fill_nulls(df, {"col1": "default"})
    assert result.first()["col1"] == "default"


def test_normalize_posting_period(spark):
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    df = spark.createDataFrame([("05/2022",), ("12/2023",)], ["invoice_posting_period"])
    result = transformations.normalize_posting_period(df, "invoice_posting_period", "posting_date")
    formatted = result.select(date_format("posting_date", "yyyy-MM-dd").alias("formatted_date"))
    actual_dates = [row["formatted_date"] for row in formatted.collect()]
    assert actual_dates == ["2022-05-01", "2023-12-01"]


def test_replace_values_replaces_multiple_values(spark):
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["category"])
    replacements = {"a": "alpha", "b": "beta"}
    result = transformations.replace_values(df, "category", replacements)
    actual = [row["category"] for row in result.collect()]
    assert actual == ["alpha", "beta", "c"]
