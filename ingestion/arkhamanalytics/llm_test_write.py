from pathlib import Path
import openai
from arkhamanalytics.prompt_engine import get_prompt_for_module

# Load OpenAI key from Azure Key Vault (via Databricks secrets)
try:
    from pyspark.dbutils import DBUtils  # For type hints if needed
except ImportError:
    DBUtils = None

# Use existing dbutils if available (Databricks), else raise error
if "dbutils" not in globals() or dbutils is None:
    raise RuntimeError("dbutils is not available. This script must run in Databricks.")

openai.api_key = dbutils.secrets.get(scope="azure-secrets", key="open-ai-api-token")


def call_llm(prompt: str, model: str = "gpt-4") -> str:
    """Call OpenAI with the given prompt and return the response text."""
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


def generate_test_file(module_path: Path, output_dir: Path) -> Path:
    """Generate a test file using OpenAI and write it to ingestion/tests."""
    prompt = get_prompt_for_module(module_path)
    print(f"📤 Sending prompt to LLM for: {module_path.name}")
    test_code = call_llm(prompt)

    test_filename = f"test_{module_path.stem}.py"
    test_path = output_dir / test_filename
    test_path.write_text(test_code)

    print(f"✅ Test file written: {test_path}")
    return test_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python scripts/llm_test_writer.py path/to/module.py")
        sys.exit(1)

    module_file = Path(sys.argv[1]).resolve()
    output_folder = Path("ingestion/tests").resolve()

    if not module_file.exists():
        print(f"❌ File not found: {module_file}")
        sys.exit(1)

    output_folder.mkdir(parents=True, exist_ok=True)
    generate_test_file(module_file, output_folder)
