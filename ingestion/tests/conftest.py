import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    """Provide a session-wide SparkSession with Delta enabled."""
    builder = SparkSession.builder \
        .appName("IngestionTests") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()