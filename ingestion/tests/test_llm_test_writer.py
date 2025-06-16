from pathlib import Path
from unittest.mock import patch, MagicMock
from arkhamanalytics.llm_test_writer import generate_test_file

@patch("arkhamanalytics.llm_test_writer.OpenAI")
def test_generate_test_file_mocked(mock_openai):
    # Mock message.content structure properly
    mock_choice = MagicMock()
    mock_message = MagicMock()
    mock_message.content = "# mock test content"
    mock_choice.message = mock_message

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value.choices = [mock_choice]
    mock_openai.return_value = mock_client

    # Setup paths
    module_path = Path("ingestion/arkhamanalytics/example_module.py")
    output_dir = Path("ingestion/tests")
    test_output_path = output_dir / "test_example_module.py"

    # Ensure clean state
    if test_output_path.exists():
        test_output_path.unlink()

    # Run and validate
    generate_test_file(module_path, output_dir, skip_if_exists=False)
    assert test_output_path.exists()
    assert "# mock test content" in test_output_path.read_text()
