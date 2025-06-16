import pytest
from pathlib import Path
from arkhamanalytics.prompt_engine import get_prompt_for_module

def test_prompt_contains_core_instructions(tmp_path):
    test_file = tmp_path / "dummy_module.py"
    test_file.write_text("def foo(): pass")

    prompt = get_prompt_for_module(test_file)
    assert "pytest" in prompt
    assert "black" in prompt
    assert "flake8" in prompt
    assert "unit tests" in prompt