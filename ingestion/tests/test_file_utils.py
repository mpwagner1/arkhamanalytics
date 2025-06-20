import pytest
from unittest.mock import patch, mock_open, MagicMock
from arkhamanalytics.file_utils import (
    detect_file_encoding,
    read_file_as_df,
    detect_and_read_file,
    resolve_file_path_for_spark,
    get_file_extension
)
from pyspark.sql import SparkSession
import chardet

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

def test_detect_file_encoding_local_file():
    mock_data = b"hello world"
    with patch("builtins.open", mock_open(read_data=mock_data)), \
         patch("arkhamanalytics.file_utils.exists", return_value=True), \
         patch("chardet.detect", return_value={"encoding": "ascii"}):
        encoding = detect_file_encoding("test.txt")
        assert encoding == "ascii"

def test_detect_file_encoding_file_not_found():
    with patch("arkhamanalytics.file_utils.exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            detect_file_encoding("nonexistent.txt")

def test_detect_file_encoding_spark_path():
    encoding = detect_file_encoding("dbfs:/mnt/test.txt")
    assert encoding == "utf-8"

def test_read_file_as_df_csv(spark_session):
    with patch("arkhamanalytics.file_utils.exists", return_value=True), \
         patch("arkhamanalytics.file_utils.detect_file_encoding", return_value="utf-8"), \
         patch.object(SparkSession, 'read', return_value=MagicMock()):
        df = read_file_as_df(spark_session, "test.csv", "csv")
        assert df is not None

def test_read_file_as_df_txt(spark_session):
    with patch("arkhamanalytics.file_utils.exists", return_value=True), \
         patch("arkhamanalytics.file_utils.detect_file_encoding", return_value="utf-8"), \
         patch.object(SparkSession, 'read', return_value=MagicMock()):
        df = read_file_as_df(spark_session, "test.txt", "txt")
        assert df is not None

def test_read_file_as_df_xlsx(spark_session):
    with patch("arkhamanalytics.file_utils.exists", return_value=True), \
         patch.object(SparkSession, 'read', return_value=MagicMock()):
        df = read_file_as_df(spark_session, "test.xlsx", "xlsx", sheet_name="Sheet1", start_cell="A1")
        assert df is not None

def test_read_file_as_df_unsupported_format(spark_session):
    with patch("arkhamanalytics.file_utils.exists", return_value=True):
        with pytest.raises(ValueError):
            read_file_as_df(spark_session, "test.unsupported", "unsupported")

def test_detect_and_read_file(spark_session):
    with patch("arkhamanalytics.file_utils.get_file_extension", return_value="csv"), \
         patch("arkhamanalytics.file_utils.read_file_as_df", return_value=MagicMock()):
        df = detect_and_read_file(spark_session, "test.csv")
        assert df is not None

def test_resolve_file_path_for_spark():
    with patch("arkhamanalytics.file_utils.glob", return_value=["/dbfs/mnt/container/test.csv"]):
        path = resolve_file_path_for_spark("container", "test.csv")
        assert path == "dbfs:/mnt/container/test.csv"

def test_resolve_file_path_for_spark_no_match():
    with patch("arkhamanalytics.file_utils.glob", return_value=[]):
        with pytest.raises(FileNotFoundError):
            resolve_file_path_for_spark("container", "nonexistent.csv")

def test_get_file_extension():
    assert get_file_extension("test.csv") == "csv"
    assert get_file_extension("test.xlsx") == "xlsx"
    assert get_file_extension("test") == ""