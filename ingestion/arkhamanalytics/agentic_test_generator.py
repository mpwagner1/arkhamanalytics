import subprocess
from pathlib import Path
from arkhamanalytics.prompt_engine import get_prompt_for_module

def get_changed_modules() -> list[Path]:
    """Detect Python modules changed in the last commit (normalized for GitHub Actions)."""
    repo_root = Path(__file__).resolve().parents[2]  # repo root
    result = subprocess.run(
        ["git", "log", "--name-only", "-1", "--pretty=format:"],
        capture_output=True,
        text=True,
        check=False,
    )
    changed_files = result.stdout.strip().splitlines()

    changed_modules = []

    for f in changed_files:
        full_path = (repo_root / f).resolve()
        if not full_path.exists():
            continue

        rel_path = full_path.relative_to(repo_root)
        if (
            rel_path.suffix == ".py"
            and "test_" not in rel_path.name
            and "scripts" not in str(rel_path)
        ):
            changed_modules.append(full_path)
    return changed_modules


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
