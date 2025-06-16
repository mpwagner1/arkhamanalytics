from pathlib import Path
import openai
from arkhamanalytics.prompt_engine import get_prompt_for_module

# Use global dbutils if available (Databricks), but defer error until used
try:
    dbutils  # noqa: F821
except NameError:
    dbutils = None

try:
    from pyspark.dbutils import DBUtils  # noqa: F401
except ImportError:
    DBUtils = None


def _load_openai_key():
    """Only call this when running in Databricks."""
    if dbutils is None:
        raise RuntimeError("dbutils not available. Run this in a Databricks notebook environment.")
    return dbutils.secrets.get(scope="azure-secrets", key="open-ai-api-token")

def call_llm(prompt: str, model: str = "gpt-4") -> str:
    """Call OpenAI with the given prompt and return the response text."""
    openai.api_key = _load_openai_key()

    response = openai.ChatCompletion.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are an expert Python test engineer."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=1800,
    )
    return response["choices"][0]["message"]["content"].strip()

def generate_test_file(module_path: Path, output_dir: Path, skip_if_exists: bool = True) -> Path:
    """Generate a test file using OpenAI and write it to ingestion/tests."""
    test_filename = f"test_{module_path.stem}.py"
    test_path = output_dir / test_filename

    if skip_if_exists and test_path.exists():
        print(f"⚠️ Skipping {test_path.name} (already exists).")
        return test_path

    prompt = get_prompt_for_module(module_path)
    print(f"📤 Sending prompt to LLM for: {module_path.name}")
    test_code = call_llm(prompt)
    test_path.write_text(test_code)

    print(f"Test file written: {test_path}")
    return test_path

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python scripts/llm_test_writer.py path/to/module.py")
        sys.exit(1)

    module_file = Path(sys.argv[1]).resolve()
    output_folder = Path("ingestion/tests").resolve()

    if not module_file.exists():
        print(f"File not found: {module_file}")
        sys.exit(1)

    output_folder.mkdir(parents=True, exist_ok=True)
    generate_test_file(module_file, output_folder)
