import logging
from typing import Any, Dict, Optional
from dataclasses import dataclass

try:
    from pyspark.dbutils import DBUtils  # for type hints in IDEs
except ImportError:
    DBUtils = None

try:
    dbutils  # Databricks runtime global
except NameError:
    dbutils = None  # allow mocking or unit testing

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class WidgetManager:
    def __init__(self):
        self.widgets: Dict[str, str] = {}

    def create(
        self,
        name: str,
        default_value: str,
        widget_type: str = "text",
        choices: Optional[list] = None,
    ):
        """Create a Databricks widget if it doesn't exist already."""
        if widget_type == "text":
            dbutils.widgets.text(name, default_value)
        elif widget_type == "dropdown" and choices:
            dbutils.widgets.dropdown(name, default_value, choices)
        elif widget_type == "combobox" and choices:
            dbutils.widgets.combobox(name, default_value, choices)
        elif widget_type == "multiselect" and choices:
            dbutils.widgets.multiselect(name, default_value, choices)
        else:
            raise ValueError(
                f"Unsupported widget type or missing choices for: {name}"
            )

        self.widgets[name] = dbutils.widgets.get(name)
        logger.info(f"Widget created: {name} = {self.widgets[name]}")

    def get(self, name: str, cast_type: Optional[str] = None) -> Any:
        """Retrieve widget value with optional casting."""
        value = dbutils.widgets.get(name)

        if cast_type == "int":
            return int(value)
        if cast_type == "float":
            return float(value)
        if cast_type == "bool":
            return value.lower() in ("true", "1", "yes")

        return value  # default is str

    def get_all(self) -> Dict[str, str]:
        """Return all widgets as a dictionary of raw string values."""
        return {
            item: dbutils.widgets.get(item)
            for item in dbutils.widgets.getArgumentNames()
        }

    def as_config_dict(
        self, cast_map: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Returns widgets as config-ready dict with casting support.
        Example cast_map: {'batch_size': 'int', 'enabled': 'bool'}
        """
        result = {}
        for name in dbutils.widgets.getArgumentNames():
            cast_type = cast_map.get(name) if cast_map else None
            result[name] = self.get(name, cast_type)
        return result

    def remove_all(self):
        """Remove all widgets (useful for reruns)."""
        for name in dbutils.widgets.getArgumentNames():
            dbutils.widgets.remove(name)
        logger.info("All widgets removed.")


@dataclass
class ProcessingConfig:
    container_name: str
    file_pattern: str
    encoding: str
    delimiter: str
    quotechar: str
    escapechar: str
    skip_lines: int
    audit_table: str
    sheet_name: str
    excel_starting_cell: str


def get_config_from_widgets() -> ProcessingConfig:
    """Fetch widget values and return them as a typed ProcessingConfig object."""
    try:
        skip_lines = int(dbutils.widgets.get("skip_lines"))
    except Exception:
        skip_lines = 0

    return ProcessingConfig(
        container_name=dbutils.widgets.get("container_name"),
        file_pattern=dbutils.widgets.get("file_pattern"),
        encoding=dbutils.widgets.get("file_encoding"),
        delimiter=dbutils.widgets.get("file_delimiter"),
        quotechar=dbutils.widgets.get("file_quotechar"),
        escapechar=dbutils.widgets.get("file_escapechar"),
        skip_lines=skip_lines,
        audit_table=dbutils.widgets.get("audit_table"),
        sheet_name=dbutils.widgets.get("sheet_name"),
        excel_starting_cell=dbutils.widgets.get(
            "start_cell"
        ),
    )
