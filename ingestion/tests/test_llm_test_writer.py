from pathlib import Path
from unittest.mock import patch, MagicMock
from arkhamanalytics.llm_test_writer import generate_test_file

@patch("arkhamanalytics.llm_test_writer.OpenAI")
def test_generate_test_file_mocked(mock_openai):
    # Mock the response from OpenAI client
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value.choices = [
        MagicMock(message={"content": "# mock test content"})
    ]
    mock_openai.return_value = mock_client

    # Set up test input/output
    module_path = Path("ingestion/arkhamanalytics/example_module.py")
    output_dir = Path("ingestion/tests")

    # Make sure it doesn't already exist
    test_output_path = output_dir / "test_example_module.py"
    if test_output_path.exists():
        test_output_path.unlink()

    generate_test_file(module_path, output_dir, skip_if_exists=False)

    # Assert file was created and contains the mocked content
    assert test_output_path.exists()
    with open(test_output_path) as f:
        contents = f.read()
    assert "# mock test content" in contents

    # Clean up
    test_output_path.unlink()
