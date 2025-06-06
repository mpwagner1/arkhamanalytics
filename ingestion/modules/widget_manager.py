from typing import Any, Dict, Optional
from datetime import datetime
import logging

try:
    from pyspark.dbutils import DBUtils  # optional fallback
except ImportError:
    DBUtils = None

try:
    dbutils  # Databricks global
except NameError:
    dbutils = None  # allow mock injection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class WidgetManager:
    def __init__(self):
        self.widgets: Dict[str, str] = {}

    def create(self, name: str, default_value: str, widget_type: str = "text", choices: Optional[list] = None):
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
            raise ValueError(f"Unsupported widget type or missing choices for: {name}")
        self.widgets[name] = dbutils.widgets.get(name)
        logger.info(f"Widget created: {name} = {self.widgets[name]}")

    def get(self, name: str, cast_type: Optional[str] = None) -> Any:
        """Retrieve widget value with optional casting."""
        value = dbutils.widgets.get(name)

        if cast_type == "int":
            return int(value)
        elif cast_type == "float":
            return float(value)
        elif cast_type == "bool":
            return value.lower() in ("true", "1", "yes")
        return value  # default is str

    def get_all(self) -> Dict[str, str]:
        """Return all widgets as a dictionary of raw string values."""
        return {item: dbutils.widgets.get(item) for item in dbutils.widgets.getArgumentNames()}

    def as_config_dict(self, cast_map: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Returns widgets as config-ready dict with casting support.
        Example cast_map: {'batch_size': 'int', 'enabled': 'bool'}
        """
        result = {}
        for name in dbutils.widgets.getArgumentNames():
            value = dbutils.widgets.get(name)
            cast_type = cast_map.get(name) if cast_map else None
            result[name] = self.get(name, cast_type)
        return result

    def remove_all(self):
        """Remove all widgets (useful for reruns)."""
        for name in dbutils.widgets.getArgumentNames():
            dbutils.widgets.remove(name)
        logger.info("All widgets removed.")
