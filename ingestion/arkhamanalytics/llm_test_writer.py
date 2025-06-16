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
        if (
            "from your_module" in line
            or "from your_module_name" in line
            or "from example_module" in line
            or "from module_name" in line
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
        print(f"Skipping existing test: {output_path}")
        return

    if not os.getenv("OPENAI_API_KEY"):
        raise EnvironmentError("OPENAI_API_KEY not set in environment.")

    with open(module_path, "r") as f:
        module_code = f.read()

    prompt = (
        f"Write only the pytest unit tests as valid Python code for the following module. "
        f"Use `from arkhamanalytics.{module_name} import ...` for all function imports. "
        f"Do not include markdown formatting, explanations, or commentary. "
        f"Only output the raw test code, suitable for saving directly to a `.py` file.\n\n"
        f"{module_code}"
    )

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    print(f"Sending prompt to LLM for: {module_name}.py")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that writes clean Pytest unit tests."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    test_code = response.choices[0].message.content
    test_code = replace_module_name(test_code, module_path)

    with open(output_path, "w") as out_file:
        out_file.write(test_code)

    print(f"Test file written to: {output_path}")
