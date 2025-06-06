import pytest
from arkhamanalytics.modules.file_utils import get_file_extension, detect_file_encoding


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
