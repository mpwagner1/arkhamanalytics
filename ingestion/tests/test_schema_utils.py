import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from modules import schema_utils

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("SchemaTests").getOrCreate()

@pytest.fixture
def sample_df(spark):
    data = [("ABC", "2024-01-01", 100.5)]
    schema = StructType([
        StructField("GC_PORTCO_ID", StringType(), True),
        StructField("INVOICE_DATE", StringType(), True),
        StructField("INVOICE_NET_AMOUNT", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

def test_get_column_names_returns_expected(sample_df):
    assert schema_utils.get_column_names(sample_df) == [
        "GC_PORTCO_ID", "INVOICE_DATE", "INVOICE_NET_AMOUNT"
    ]

@pytest.mark.parametrize("expected,case_sensitive,valid,missing,unexpected", [
    (["GC_PORTCO_ID", "INVOICE_DATE", "INVOICE_NET_AMOUNT"], True, True, [], []),
    (["gc_portco_id", "invoice_date", "invoice_net_amount"], False, True, [], []),
    (["GC_PORTCO_ID", "POSTING_DATE"], True, False, ["POSTING_DATE"], ["INVOICE_DATE", "INVOICE_NET_AMOUNT"]),
])
def test_validate_column_names(sample_df, expected, case_sensitive, valid, missing, unexpected):
    result = schema_utils.validate_column_names(sample_df, expected, case_sensitive)
    assert result[0] == valid
    assert set(result[1]) == set(missing)
    assert set(result[2]) == set(unexpected)

def test_validate_column_types_matches_expected(sample_df):
    expected_schema = {
        "GC_PORTCO_ID": "string",
        "INVOICE_DATE": "string",
        "INVOICE_NET_AMOUNT": "double"
    }
    valid, mismatches, missing = schema_utils.validate_column_types(sample_df, expected_schema)
    assert valid is True
    assert mismatches == {}
    assert missing == {}

def test_validate_column_types_detects_mismatch(sample_df):
    expected_schema = {
        "GC_PORTCO_ID": "string",
        "INVOICE_DATE": "string",
        "INVOICE_NET_AMOUNT": "integer"
    }
    valid, mismatches, missing = schema_utils.validate_column_types(sample_df, expected_schema)
    assert valid is False
    assert "INVOICE_NET_AMOUNT" in mismatches
    assert missing == {}

def test_validate_column_types_detects_missing_column(sample_df):
    expected_schema = {
        "GC_PORTCO_ID": "string",
        "POSTING_DATE": "date"
    }
    valid, mismatches, missing = schema_utils.validate_column_types(sample_df, expected_schema)
    assert valid is False
    assert "POSTING_DATE" in missing
    assert mismatches == {}

def test_compare_schemas_detects_differences(spark, sample_df):
    # Different schema
    schema_b = StructType([
        StructField("GC_PORTCO_ID", StringType(), True),
        StructField("INVOICE_DATE", StringType(), True),
        StructField("POSTING_DATE", StringType(), True)
    ])
    df2 = spark.createDataFrame([("ABC", "2024-01-01", "2024-01-31")], schema=schema_b)

    result = schema_utils.compare_schemas(sample_df, df2)
    assert "INVOICE_NET_AMOUNT" in result["only_in_df1"]
    assert "POSTING_DATE" in result["only_in_df2"]
    assert result["mismatched_types"] == []
