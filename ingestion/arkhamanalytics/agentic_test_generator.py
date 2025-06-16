import subprocess
from pathlib import Path
from arkhamanalytics.prompt_engine import get_prompt_for_module


def get_changed_modules() -> list[Path]:
    """Detect Python files changed in last commit (for GitHub UI commits)."""
    result = subprocess.run(
        ["git", "log", "--name-only", "-1", "--pretty=format:"],
        capture_output=True,
        text=True,
        check=False,
    )
    changed_files = result.stdout.strip().splitlines()
    return [
        Path(f)
        for f in changed_files
        if f.endswith(".py") and "test_" not in f and "scripts/" not in f
    ]

def main():
    changed_modules = get_changed_modules()
    if not changed_modules:
        print("No Python module changes detected.")
        return

    for module_path in changed_modules:
        print(f"\nProcessing: {module_path}")
        try:
            prompt = get_prompt_for_module(module_path)
            print(f"\nPrompt Preview for {module_path.name}:\n")
            print(prompt[:1000])
            print("...prompt truncated...\n")
        except Exception as e:
            print(f"❌ Failed to generate prompt for {module_path.name}: {e}")


if __name__ == "__main__":
    main()
