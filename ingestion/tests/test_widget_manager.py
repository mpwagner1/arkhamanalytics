import pytest
from unittest.mock import MagicMock, patch
from modules.widget_manager import WidgetManager

@pytest.fixture
def mock_dbutils():
    """Fixture to patch dbutils.widgets for all tests in this file."""
    with patch("modules.widget_manager.dbutils") as mock:
        mock.widgets.text = MagicMock()
        mock.widgets.dropdown = MagicMock()
        mock.widgets.combobox = MagicMock()
        mock.widgets.multiselect = MagicMock()
        mock.widgets.get = MagicMock(side_effect=lambda name: {
            "batch_size": "1000",
            "debug": "true",
            "env": "prod"
        }[name])
        mock.widgets.getArgumentNames.return_value = ["batch_size", "debug", "env"]
        mock.widgets.remove = MagicMock()
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