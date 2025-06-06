---

## 🚀 How It Works

1. **Read input files** with flexible encoding and format support  
2. **Clean and transform** input columns using standardized logic  
3. **Validate schema** against expected structure  
4. **Log results** to a centralized Delta audit table  
5. **Drive configurations** via Databricks widgets for full reusability  

---

## 🔧 Key Modules

| Module              | Purpose |
|---------------------|---------|
| `file_utils.py`     | Detect file type/encoding and read files as Spark DataFrames |
| `schema_utils.py`   | Validate column names and types, compare to existing schemas |
| `audit_logger.py`   | Write success/failure logs to a Delta-based audit table |
| `transformations.py`| Currency cleaning, null handling, date normalization |
| `widget_manager.py` | Central interface for creating and casting Databricks widgets |

---

## 🧪 Testing and Linting

This project is continuously tested using GitHub Actions. Each push and pull request to `main` will:

- ✅ Run `pytest` to validate all unit tests  
- ✅ Use `black` to enforce consistent code formatting (auto-fixes if needed)  
- ✅ Use `flake8` to lint the codebase against style and syntax issues  

### ✅ Local Development Setup

To match CI/CD enforcement locally, install [pre-commit](https://pre-commit.com/):

```bash
pip install pre-commit
pre-commit install
