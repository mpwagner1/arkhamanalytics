import argparse
from pathlib import Path
from arkhamanalytics.agentic_test_generator import get_changed_modules
from arkhamanalytics.llm_test_writer import generate_test_file

def main():
    parser = argparse.ArgumentParser(description="Generate tests for changed modules using LLM.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing test files.")
    args = parser.parse_args()

    base_dir = Path("ingestion/arkhamanalytics")
    output_dir = Path("ingestion/tests")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("🔍 Detecting changed modules...")
    changed = get_changed_modules()

    if not changed:
        print("✅ No new or changed modules detected.")
    else:
        for module in changed:
            print(f"🧠 Generating test for: {module}")
            try:
                generate_test_file(module, output_dir, skip_if_exists=not args.force)
            except Exception as e:
                print(f"❌ Failed for {module.name}: {e}")

if __name__ == "__main__":
    main()
