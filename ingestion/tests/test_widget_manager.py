import pytest
from unittest.mock import MagicMock, patch
from arkhamanalytics.widget_manager import WidgetManager, get_config_from_widgets, ProcessingConfig
from arkhamanalytics.widget_presets import create_base_widgets

@pytest.fixture
def mock_dbutils():
    """Fixture to patch dbutils.widgets for all tests in this file."""
    with patch("arkhamanalytics.widget_manager.dbutils") as mock:
        mock.widgets.text = MagicMock()
        mock.widgets.dropdown = MagicMock()
        mock.widgets.combobox = MagicMock()
        mock.widgets.multiselect = MagicMock()
        mock.widgets.remove = MagicMock()

        widget_values = {
            "batch_size": "1000",
            "debug": "true",
            "env": "prod",
            "container_name": "raw",
            "file_pattern": "*.csv",
            "file_encoding": "utf-8",
            "file_delimiter": ",",
            "file_quotechar": '"',
            "file_escapechar": "\\",
            "skip_lines": "0",
            "audit_table": "default.audit_log",
            "sheet_name": "Sheet1",
            "start_cell": "A1"
        }

        mock.widgets.get = MagicMock(side_effect=lambda name: widget_values[name])
        mock.widgets.getArgumentNames.return_value = list(widget_values.keys())
        yield mock

def test_create_text_widget(mock_dbutils):
    wm = WidgetManager()
    wm.create("batch_size", "1000")
    mock_dbutils.widgets.text.assert_called_once_with("batch_size", "1000")

def test_create_dropdown_widget(mock_dbutils):
    wm = WidgetManager()
    wm.create("env", "prod", widget_type="dropdown", choices=["dev", "qa", "prod"])
    mock_dbutils.widgets.dropdown.assert_called_once()

def test_get_casts_value_correctly(mock_dbutils):
    wm = WidgetManager()
    assert wm.get("batch_size", "int") == 1000
    assert wm.get("debug", "bool") is True
    assert wm.get("env") == "prod"

def test_get_all_returns_all_widget_values(mock_dbutils):
    wm = WidgetManager()
    result = wm.get_all()
    assert result == {
        "batch_size": "1000",
        "debug": "true",
        "env": "prod"
    }

def test_as_config_dict_casts_properly(mock_dbutils):
    wm = WidgetManager()
    cast_map = {"batch_size": "int", "debug": "bool", "env": "str"}
    config = wm.as_config_dict(cast_map)
    assert config == {
        "batch_size": 1000,
        "debug": True,
        "env": "prod"
    }

def test_remove_all_calls_remove(mock_dbutils):
    wm = WidgetManager()
    wm.remove_all()
    assert mock_dbutils.widgets.remove.call_count == 3

def test_create_base_widgets_sets_expected(mock_dbutils):
    wm = WidgetManager()
    create_base_widgets(wm)

    call_args = [call[0][0] for call in mock_dbutils.widgets.get.call_args_list]
    actual_keys = set(call_args)

    expected_keys = {
        "container_name", "file_pattern", "file_encoding", "file_delimiter",
        "file_quotechar", "file_escapechar", "skip_lines", "audit_table",
        "sheet_name", "start_cell"
    }

    assert expected_keys.issubset(actual_keys)
