import os
from pathlib import Path
from openai import OpenAI
from arkhamanalytics.prompt_engine import get_prompt_for_module

client = OpenAI()

def generate_test_file(module_path: Path, output_dir: Path, skip_if_exists: bool = True) -> None:
    """
    Generate a unit test file for a given module using OpenAI's LLM.
    Args:
        module_path (Path): Path to the Python module to test.
        output_dir (Path): Directory to save the generated test file.
        skip_if_exists (bool): If True, skip generation if test file already exists.
    """
    module_name = module_path.stem
    test_filename = f"test_{module_name}.py"
    test_file_path = output_dir / test_filename

    if skip_if_exists and test_file_path.exists():
        print(f"Test file already exists: {test_filename}")
        return

    print(f"Sending prompt to LLM for: {module_name}")
    prompt = get_prompt_for_module(module_path)

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert Python test engineer."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )

        generated = response.choices[0].message.content

        # Extract code block (optional: could be improved with regex if needed)
        code_block = generated.split("```python")[-1].split("```")[0].strip()

        test_file_path.write_text(code_block)
        print(f"Test written to: {test_file_path.relative_to(Path.cwd())}")

    except Exception as e:
        print(f"Failed for {module_name}: {e}")
