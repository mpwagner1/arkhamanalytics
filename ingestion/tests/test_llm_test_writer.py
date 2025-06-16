from arkhamanalytics.llm_test_writer import generate_test_file

@patch("arkhamanalytics.llm_test_writer.call_llm")
def test_generate_test_file_creates_output(mock_call_llm, tmp_path):
    mock_call_llm.return_value = "# sample test code"
    dummy_module = tmp_path / "dummy_module.py"
    dummy_module.write_text("def bar(): return 42")

    output_path = generate_test_file(dummy_module, tmp_path)
    assert output_path.exists()
    assert output_path.read_text() == "# sample test code"
