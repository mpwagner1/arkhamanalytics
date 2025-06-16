import os
from pathlib import Path
from openai import OpenAI


def replace_module_name(code: str, module_path: Path) -> str:
    """Replace placeholder import with the actual module path."""
    module_name = module_path.stem
    import_path = f"arkhamanalytics.{module_name}"
    lines = code.splitlines()
    updated_lines = []
    for line in lines:
        if line.strip().startswith("from ") and (
            "your_module" in line or "your_module_name" in line or "example_module" in line
        ):
            updated_lines.append(f"from {import_path} import (")
        else:
            updated_lines.append(line)
    return "\n".join(updated_lines)


def generate_test_file(module_path: Path, output_dir: Path, skip_if_exists: bool = True) -> None:
    """Generate a Pytest-compatible test file using OpenAI for a given module."""
    module_name = module_path.stem
    test_file_name = f"test_{module_name}.py"
    output_path = output_dir / test_file_name

    if skip_if_exists and output_path.exists():
        print(f"🟡 Skipping existing test: {output_path}")
        return

    if not os.getenv("OPENAI_API_KEY"):
        raise EnvironmentError("❌ OPENAI_API_KEY not set in environment.")

    with open(module_path, "r") as f:
        module_code = f.read()

    prompt = f"Write pytest unit tests for the following Python module:\n\n```python\n{module_code}\n```"

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    print(f"📤 Sending prompt to LLM for: {module_name}.py")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that writes clean Pytest unit tests."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    # Safely extract content
    try:
        test_code = response.choices[0].message.content
    except (AttributeError, IndexError, KeyError) as e:
        raise ValueError(f"❌ Failed to parse LLM response: {e}")

    test_code = replace_module_name(test_code, module_path)

    with open(output_path, "w") as out_file:
        out_file.write(test_code)

    print(f"✅ Test file written to: {output_path}")
