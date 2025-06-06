import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import date_format
from modules import transformations

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TransformTests").getOrCreate()

def test_standardize_column_names(spark):
    df = spark.createDataFrame([("abc",)], ["  Supplier Name  "])
    result = transformations.standardize_column_names(df)
    assert result.columns == ["supplier_name"]

def test_clean_text_column_removes_whitespace_and_lowercases(spark):
    df = spark.createDataFrame([("  HeLLo WoRLD ",)], ["text"])
    result = transformations.clean_text_column(df, "text")
    assert result.collect()[0]["text"] == "hello world"

def test_remove_currency_symbols_parses_float(spark):
    df = spark.createDataFrame([("$1,234.56",)], ["amount"])
    result = transformations.remove_currency_symbols(df, "amount")
    assert result.collect()[0]["amount"] == pytest.approx(1234.56, rel=1e-4)

def test_fill_nulls_applies_correctly(spark):
    df = spark.createDataFrame([("data",), (None,)], ["desc"])
    result = transformations.fill_nulls(df, ["desc"], "Unknown")
    assert result.filter("desc = 'Unknown'").count() == 1

def test_normalize_posting_period(spark):
    df = spark.createDataFrame(
        [("05/2022",), ("12/2023",)], ["invoice_posting_period"]
    )

    result = transformations.normalize_posting_period(
        df, "invoice_posting_period", "posting_date"
    )

    formatted = result.select(date_format("posting_date", "yyyy-MM-dd").alias("formatted_date"))
    actual_dates = [row["formatted_date"] for row in formatted.collect()]

    assert actual_dates == ["2022-05-01", "2023-12-01"]

def test_replace_values_replaces_multiple_values(spark):
    df = spark.createDataFrame([("N/A",), ("null",), ("actual",)], ["supplier"])
    replacements = {"N/A": None, "null": None}
    result = transformations.replace_values(df, "supplier", replacements)

    rows = result.collect()
    assert rows[0]["supplier"] is None
    assert rows[1]["supplier"] is None
    assert rows[2]["supplier"] == "actual"
