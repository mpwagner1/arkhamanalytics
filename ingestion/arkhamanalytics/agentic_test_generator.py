import subprocess
from pathlib import Path
from arkhamanalytics.prompt_engine import get_prompt_for_module


def get_changed_modules(base_dir: Path) -> list[Path]:
    """Detects Python files changed in the most recent commit."""
    result = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    changed_files = result.stdout.strip().splitlines()
    return [
        base_dir / f
        for f in changed_files
        if f.endswith(".py") and "test_" not in f and "scripts/" not in f
    ]

def main():
    base_dir = Path(__file__).resolve().parent.parent
    changed_modules = get_changed_modules(base_dir)

    if not changed_modules:
        print("✅ No Python module changes detected.")
        return

    for module_path in changed_modules:
        print(f"\n📦 Processing: {module_path}")
        try:
            prompt = get_prompt_for_module(module_path)
            print(f"\n🧠 Prompt Preview for {module_path.name}:\n")
            print(prompt[:1000])  # Preview first 1000 chars
            print("...prompt truncated...\n")
        except Exception as e:
            print(f"❌ Failed to generate prompt for {module_path.name}: {e}")


if __name__ == "__main__":
    main()
