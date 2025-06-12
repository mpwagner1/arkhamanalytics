# ArkhamAnalytics Databricks Project

[![codecov](https://codecov.io/gh/mpwagner1/arkhamanalytics/branch/main/graph/badge.svg)](https://codecov.io/gh/mpwagner1/arkhamanalytics)

## 🧩 Structure
- `modules/`: Reusable Python modules (e.g., mounting, logging)
- `notebooks/`: Operational notebooks for running pipelines and workflows
- `tests/`: Unit tests for core modules

## 🛠 Setup
1. Clone repo in Databricks
2. Configure secret scopes for access keys
3. Run `notebooks/init_mount_test` to verify config

## 🔐 Secrets
Store secrets in Databricks via:
```python
dbutils.secrets.get(scope="azure-secrets", key="my-key-name")
