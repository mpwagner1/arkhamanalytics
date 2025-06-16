from pathlib import Path

def get_prompt_for_module(module_path: Path) -> str:
    """Generate a custom prompt for an LLM to create pytest-based tests for a given module."""
    code = module_path.read_text()

    # Detect usage patterns
    uses_spark = "SparkSession" in code or "DataFrame" in code
    uses_dbutils = "dbutils" in code
    uses_delta = (
        "delta" in code or "saveAsTable" in code or 'format("delta")' in code
    )

    # Base context
    prompt_intro = (
        "You are generating unit tests for a modular ingestion framework "
        "written in Python."
    )
    instructions = [
        "Use `pytest` for writing unit tests.",
        "Ensure coverage is collected using `pytest-cov`.",
        "Cover normal logic paths and edge cases.",
        "Format test code to pass `black --check`.",
        "Lint test code to pass `flake8` with max line length of 100.",
        "Organize test files under `ingestion/tests/` with `test_*.py` naming.",
        "Use mocking where appropriate (e.g., `dbutils`, Spark, file system).",
        "Assume tests will run in CI using Python 3.10 on Ubuntu via GitHub Actions.",
    ]

    # Tailor based on detected features
    if uses_spark:
        instructions.append(
            "Use PySpark's `SparkSession` and small test DataFrames as fixtures."
        )
    if uses_dbutils:
        instructions.append(
            "Mock `dbutils.widgets` using `unittest.mock`."
        )
    if uses_delta:
        instructions.append(
            "If Delta Lake writes are used, simulate file-based writes "
            "using a temp path or mock them."
        )

    # Final prompt
    full_prompt = (
        f"{prompt_intro}\n\n"
        + "\n".join(instructions)
        + "\n\nHere is the source code for the module:\n\n"
        + "```python\n"
        + code
        + "\n```"
    )

    return full_prompt
