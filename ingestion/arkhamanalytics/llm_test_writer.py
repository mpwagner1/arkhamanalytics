import os
import ast
from pathlib import Path
from openai import OpenAI


def replace_module_name(code: str, module_path: Path) -> str:
    """Replace placeholder import with the actual module path."""
    module_name = module_path.stem
    import_path = f"arkhamanalytics.{module_name}"
    lines = code.splitlines()
    updated_lines = []
    for line in lines:
        if any(keyword in line for keyword in ["from your_module", "from your_module_name", "from example_module", "from module_name"]):
            updated_lines.append(f"from {import_path} import (")
        else:
            updated_lines.append(line)
    return "\n".join(updated_lines)


def is_valid_python(code: str) -> bool:
    """Check if the generated code is syntactically valid."""
    try:
        ast.parse(code)
        return True
    except SyntaxError as e:
        print(f"Syntax error in generated code: {e}")
        return False


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
    f"You are a senior Python test engineer. Generate only raw Python `pytest` test code "
    f"for the given module.\n\n"
    f"Guidelines:\n"
    f"- DO NOT include Markdown formatting (no triple backticks).\n"
    f"- Import from `arkhamanalytics.{module_name}`.\n"
    f"- Use `pytest` + `unittest.mock` to mock all external I/O dependencies "
    f"(e.g., file reads, `open()`, Spark `.read`, or `os.path.exists`).\n"
    f"- Patch any functions that would access the real file system "
    f"(like `open()` or `detect_file_encoding()`).\n"
    f"- Assume all external interactions should be mocked to run in isolation.\n"
    f"- Do not include prose, comments, or explanations.\n\n"
    f"Write the test file for this module:\n\n{module_code}"
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

    if not is_valid_python(test_code):
        raise ValueError(f"Generated test code for {module_name}.py is invalid Python.")

    with open(output_path, "w") as out_file:
        out_file.write(test_code)

    print(f"Test file written to: {output_path}")
