import os
from pathlib import Path
from openai import OpenAI

def extract_python_code(response_text: str) -> str:
    """Extract Python code block from LLM markdown output."""
    if "```python" in response_text:
        return response_text.split("```python")[1].split("```")[0].strip()
    elif "```" in response_text:
        return response_text.split("```")[1].split("```")[0].strip()
    return response_text.strip()

def generate_test_file(module_path: Path, output_dir: Path, skip_if_exists: bool = True) -> None:
    module_name = module_path.stem
    test_file_name = f"test_{module_name}.py"
    output_path = output_dir / test_file_name

    if skip_if_exists and output_path.exists():
        print(f"Skipping existing test: {output_path}")
        return

    with open(module_path, "r") as f:
        module_code = f.read()

    prompt = f"Write Pytest unit tests for the following module:\n\n{module_code}"

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    print(f"Sending prompt to LLM for: {module_name}.py")

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that writes clean Pytest unit tests.",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    # Extract the clean code block
    full_response = response.choices[0].message.content
    test_code = extract_python_code(full_response)

    with open(output_path, "w") as f:
        f.write(test_code)

    print(f"Test written to: {output_path}")
