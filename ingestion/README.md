# 🧠 ArkhamAnalytics Ingestion Framework

A modular PySpark-based ingestion framework designed for Databricks pipelines. Built with production-grade audit logging, schema validation, file ingestion, and now powered by LLM-based test automation.

---

## 🚀 How It Works

This framework follows a bronze-layer architecture pattern where raw files are ingested into Delta Lake with full transparency and traceability.

### Ingestion Flow

1. **Flexible file reading**  
   Detects encoding, delimiter, quote/escape characters, and reads into Spark DataFrames

2. **Column cleansing and normalization**  
   Handles currency symbols, nulls, date parsing, and value trimming

3. **Schema validation**  
   Validates column names and types using a schema registry or dynamic checks

4. **Audit logging**  
   Logs ingestion success/failure to a centralized Delta audit table with metadata (e.g. record counts, errors, timestamps)

5. **Widget-driven configuration**  
   Fully reusable notebook logic powered by Databricks widgets for dynamic execution

---

## 🧠 LLM-Based Test Generation

This framework includes a fully automated workflow that uses **OpenAI’s GPT-4o** model to generate unit tests for Python modules.

### 🔍 Overview

The test generation system scans modified or target modules (e.g. `example_module.py`), sends their source code to OpenAI’s API, and produces a clean, executable test file using `pytest`. 

The output file is:

- ✅ Free of markdown or explanations
- ✅ Imports the correct functions from the target module (e.g. `from arkhamanalytics.example_module import ...`)
- ✅ Covers common and edge cases
- ✅ PEP8-compliant (`flake8`, `black`)
- ✅ CI-ready with no required refactoring

### 🛠️ Files Involved

| File | Purpose |
|------|---------|
| `llm_test_writer.py` | Generates test files from modules using OpenAI |
| `run_llm_test_gen.py` | Entry point script to batch run test generation |
| `.github/workflows/test_generation.yml` | GitHub Actions workflow that executes `run_llm_test_gen.py` and creates a PR |

---

### 🧪 How It Works

1. **Module is modified or targeted for testing**  
   e.g., you push changes to `arkhamanalytics/file_utils.py`

2. **LLM Test Generator is triggered manually or on schedule**  
   Uses `workflow_dispatch` for manual runs (can be adapted for automatic triggers)

3. **Prompt construction**  
   The module code is inserted into a carefully engineered prompt:
   - Strips non-code
   - Requests only valid Python output
   - Instructs the LLM to use the correct import path (e.g., `from arkhamanalytics.file_utils import ...`)
   - Warns against markdown or prose

4. **LLM returns raw test code**  
   OpenAI response is parsed and validated. The module name in the import is replaced dynamically via `replace_module_name(...)`

5. **Test file is saved**  
   e.g., `test_file_utils.py` is saved under `ingestion/tests/`

6. **Pull Request is created**  
   The test file is committed to a new branch and submitted as a PR titled `Auto-generated Tests (LLM)`

---

### 🧩 Example Output

```python
import pytest
from arkhamanalytics.example_module import add_numbers

def test_add_numbers():
    assert add_numbers(2, 3) == 5
    assert add_numbers(-1, 1) == 0
    assert add_numbers(0, 0) == 0
```

✅ No explanations.  
✅ Ready to merge.  
✅ CI-compliant.

---

### 🔐 Requirements

- OpenAI API key stored as a GitHub Actions secret: `OPENAI_API_KEY`
- Python 3.10+ and `openai`, `pytest`, `flake8`, `black`, `pytest-cov` installed in CI

---

## 🔧 Core Modules

| Module                  | Purpose |
|--------------------------|---------|
| `file_utils.py`          | Detect file encoding and load files into Spark DataFrames |
| `schema_utils.py`        | Perform column name/type validation and compare with expected schema |
| `audit_logger.py`        | Log audit outcomes (success/failure) to a Delta Lake audit table |
| `transformations.py`     | Normalize currency, clean nulls, standardize date formats |
| `widget_manager.py`      | Manage Databricks widgets and safely cast widget inputs |
| `llm_test_writer.py`     | Generate test files automatically using OpenAI |
| `run_llm_test_gen.py`    | Entrypoint for batch-running LLM-based test generation |
| `agentic_test_generator.py` | (Experimental) Hooks for agent-based testing orchestration |

---

## ✅ CI/CD Enforcement

GitHub Actions enforces:

- 🔍 `pytest` test execution with coverage  
- 🎯 `flake8` style/lint compliance  
- 🎨 `black` formatting enforcement  
- 🧠 LLM-generated test PR creation

Pull requests must pass all checks before merging.

---

## 🧑‍💻 Local Development Setup

```bash
git clone https://github.com/<your-org>/arkhamanalytics.git
cd arkhamanalytics
pip install -r ingestion/requirements-dev.txt
```

Enable pre-commit to auto-format and lint before each commit:

```bash
pip install pre-commit
pre-commit install
```

Run tests manually:

```bash
PYTHONPATH=ingestion pytest --cov=ingestion/ingestion/arkhamanalytics --cov-report=term-missing
```

---

## 🧭 Roadmap

- 🔄 Trigger LLM test generation automatically on every commit to `main`
- 🧠 Implement multi-agent validation/voting across models for test quality
- 🧪 Expand LLM tests to integration-level behaviors (e.g. Spark + Delta writes)
- 📦 Package test generation as a CLI and REST service

---

## 📜 License

MIT License. Open to internal contributions and test suite enhancements.
