from pathlib import Path
import openai
import os
from arkhamanalytics.prompt_engine import get_prompt_for_module
from arkhamanalytics.test_header_utils import build_test_header
import hashlib

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
    if dbutils is not None:
        return dbutils.secrets.get(scope="azure-secrets", key="open-ai-api-token")
    # Fallback for CI environments like GitHub Actions
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OpenAI API key not set in environment variable.")
    return key

def extract_existing_prompt_hash(test_path: Path) -> str | None:
    if not test_path.exists():
        return None
    for line in test_path.read_text().splitlines():
        if line.startswith("# Prompt SHA256:"):
            return line.split(":", 1)[1].strip()
    return None

def call_llm(prompt: str, model: str = "gpt-4o") -> str:
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
    test_filename = f"test_{module_path.stem}.py"
    test_path = output_dir / test_filename

    prompt = get_prompt_for_module(module_path)
    new_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]

    if test_path.exists():
        old_hash = extract_existing_prompt_hash(test_path)
        new_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]
    
        if skip_if_exists and old_hash == new_hash:
            print(f"✅ Skipping {test_path.name} (prompt unchanged).")
            return test_path

    header = build_test_header(prompt, module_path.name)
    print(f"📤 Sending prompt to LLM for: {module_path.name}")
    test_code = call_llm(prompt)

    test_path.write_text(f"{header}{test_code}")
    print(f"✅ Test file written: {test_path}")
    return test_path
