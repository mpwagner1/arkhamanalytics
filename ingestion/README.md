
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

## 🧪 Testing

All modules are designed for unit testing using `pytest`. You can add tests to `/tests/` and run them locally or via CI/CD.

---

## ✅ Example Ingestion Flow

```python
from file_utils import read_file_as_df
from schema_utils import validate_column_names
from transformations import clean_text_column
from audit_logger import log_ingestion_audit
from widget_manager import WidgetManager

widgets = WidgetManager()
widgets.create("file_path", "/mnt/raw/input.csv")
widgets.create("env", "dev", widget_type="dropdown", choices=["dev", "prod"])

df = read_file_as_df(spark, widgets.get("file_path"), "csv")

df = clean_text_column(df, "supplier_name")
is_valid, missing, unexpected = validate_column_names(df, expected_columns)

log_ingestion_audit(spark, "/mnt/logs/ingestion_audit/", ..., status="SUCCESS")
