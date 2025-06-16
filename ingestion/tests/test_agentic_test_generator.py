import subprocess
from pathlib import Path
from arkhamanalytics.agentic_test_generator import get_changed_modules

def test_get_changed_modules_filters_tests(monkeypatch, tmp_path):
    test_files = [
        "ingestion/arkhamanalytics/file_utils.py",
        "ingestion/tests/test_file_utils.py",
        "scripts/agentic_test_generator.py"
    ]
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: type("Result", (), {"stdout": "\n".join(test_files)}))

    changed = get_changed_modules(Path.cwd())
    assert "test_file_utils.py" not in [str(p) for p in changed]
    assert any("file_utils.py" in str(p) for p in changed)