import pytest
from unittest.mock import MagicMock, patch
from arkhamanalytics.file_utils import get_file_extension, detect_file_encoding, read_file_as_df


@pytest.mark.parametrize("filename,expected", [
    ("file.csv", "csv"),
    ("data.XLSX", "xlsx"),
    ("/path/to/file.TXT", "txt"),
    ("no_extension", "")
])
def test_get_file_extension_handles_various_cases(filename, expected):
    assert get_file_extension(filename) == expected

def test_detect_file_encoding_utf8(tmp_path):
    """Detect UTF-8 encoding from known UTF-8 file."""
    file_path = tmp_path / "utf8_test.txt"
    content = "hello world äöü".encode("utf-8")
    file_path.write_bytes(content)

    encoding = detect_file_encoding(str(file_path))
    assert isinstance(encoding, str)
    assert encoding.lower() in ["utf-8", "ascii"]

def test_detect_file_encoding_latin1(tmp_path):
    """Detect Latin-1 encoding from known file."""
    file_path = tmp_path / "latin_test.txt"
    content = "café résumé".encode("latin1")
    file_path.write_bytes(content)

    encoding = detect_file_encoding(str(file_path))
    assert isinstance(encoding, str)
    assert encoding.lower() in ["iso-8859-1", "windows-1252", "latin-1"]

def test_detect_file_encoding_raises_file_not_found():
    """Ensure detect_file_encoding raises on missing file."""
    with pytest.raises(FileNotFoundError):
        detect_file_encoding("/nonexistent/file.txt")

def test_read_excel_constructs_data_address_correctly(spark, tmp_path):
    test_file = tmp_path / "test.xlsx"
    test_file.write_text("fake content")

    with (
        patch("arkhamanalytics.file_utils.exists", return_value=True),
        patch("arkhamanalytics.file_utils.detect_file_encoding", return_value="utf-8"),
    ):
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = "mock_df"

        # Patch the .read property directly on this spark instance
        spark.read = mock_reader

        result = read_file_as_df(
            spark=spark,
            file_path=str(test_file),
            file_format="xlsx",
            encoding="utf-8",
            sheet_name="MySheet",
            start_cell="B5",
        )

        mock_reader.option.assert_any_call("dataAddress", "'MySheet'!B5")
        assert result == "mock_df"
