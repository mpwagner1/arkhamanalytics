import pytest
from unittest.mock import MagicMock
from arkhamanalytics.widget_manager import WidgetManager, get_config_from_widgets
from arkhamanalytics.widget_presets import create_base_widgets


@pytest.fixture
def mock_dbutils():
    """Fixture to mock dbutils.widgets for all tests in this file."""
    mock_widgets = MagicMock()
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

    mock_widgets.get.side_effect = lambda name: widget_values[name]
    mock_widgets.getArgumentNames.return_value = list(widget_values.keys())
    mock_widgets.remove = MagicMock()
    mock_widgets.text = MagicMock()
    mock_widgets.dropdown = MagicMock()
    mock_widgets.combobox = MagicMock()
    mock_widgets.multiselect = MagicMock()

    mock_dbutils = MagicMock()
    mock_dbutils.widgets = mock_widgets

    return mock_dbutils

def test_create_text_widget(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    wm.create("batch_size", "1000")
    mock_dbutils.widgets.text.assert_called_once_with("batch_size", "1000")

def test_create_dropdown_widget(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    wm.create("env", "prod", widget_type="dropdown", choices=["dev", "qa", "prod"])
    mock_dbutils.widgets.dropdown.assert_called_once()

def test_get_casts_value_correctly(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    assert wm.get("batch_size", "int") == 1000
    assert wm.get("debug", "bool") is True
    assert wm.get("env") == "prod"

def test_get_all_returns_all_widget_values(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    result = wm.get_all()

    expected = {
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
        "start_cell": "A1",
    }

    assert result == expected

def test_as_config_dict_casts_properly(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    cast_map = {"batch_size": "int", "debug": "bool", "env": "str"}
    config = wm.as_config_dict(cast_map)

    assert config["batch_size"] == 1000
    assert config["debug"] is True
    assert config["env"] == "prod"


def test_remove_all_calls_remove(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    wm.remove_all()
    expected_count = len(mock_dbutils.widgets.getArgumentNames.return_value)
    assert mock_dbutils.widgets.remove.call_count == expected_count

def test_create_base_widgets_sets_expected(mock_dbutils):
    wm = WidgetManager(dbutils_ref=mock_dbutils)
    create_base_widgets(wm)

    call_args = [call[0][0] for call in mock_dbutils.widgets.get.call_args_list]
    actual_keys = set(call_args)

    expected_keys = {
        "container_name", "file_pattern", "file_encoding", "file_delimiter",
        "file_quotechar", "file_escapechar", "skip_lines", "audit_table",
        "sheet_name", "start_cell"
    }

    assert expected_keys.issubset(actual_keys)

def test_get_config_from_widgets_builds_expected_config(mock_dbutils):
    config = get_config_from_widgets(mock_dbutils)
    assert config.container_name == "raw"
    assert config.sheet_name == "Sheet1"
    assert config.excel_starting_cell == "A1"
    assert config.skip_lines == 0
