import pytest
from unittest.mock import MagicMock
from arkhamanalytics.widget_manager import WidgetManager, get_config_from_widgets, ProcessingConfig

def test_widget_manager_initialization_without_dbutils():
    with pytest.raises(EnvironmentError):
        WidgetManager()

def test_widget_manager_initialization_with_dbutils():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    assert manager.dbutils == mock_dbutils

def test_create_text_widget():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    manager.create(name="test_widget", default_value="default")
    mock_dbutils.widgets.text.assert_called_once_with("test_widget", "default")

def test_create_dropdown_widget():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    manager.create(name="test_widget", default_value="choice1", widget_type="dropdown", choices=["choice1", "choice2"])
    mock_dbutils.widgets.dropdown.assert_called_once_with("test_widget", "choice1", ["choice1", "choice2"])

def test_create_combobox_widget():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    manager.create(name="test_widget", default_value="choice1", widget_type="combobox", choices=["choice1", "choice2"])
    mock_dbutils.widgets.combobox.assert_called_once_with("test_widget", "choice1", ["choice1", "choice2"])

def test_create_multiselect_widget():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    manager.create(name="test_widget", default_value="choice1", widget_type="multiselect", choices=["choice1", "choice2"])
    mock_dbutils.widgets.multiselect.assert_called_once_with("test_widget", "choice1", ["choice1", "choice2"])

def test_create_widget_invalid_type():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    with pytest.raises(ValueError):
        manager.create(name="test_widget", default_value="default", widget_type="invalid")

def test_get_widget_value():
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.get.return_value = "42"
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    assert manager.get("test_widget") == "42"

def test_get_widget_value_with_casting():
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.get.return_value = "42"
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    assert manager.get("test_widget", cast_type="int") == 42

def test_get_all_widgets():
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.getArgumentNames.return_value = ["widget1", "widget2"]
    mock_dbutils.widgets.get.side_effect = lambda name: f"value_of_{name}"
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    assert manager.get_all() == {"widget1": "value_of_widget1", "widget2": "value_of_widget2"}

def test_as_config_dict():
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.getArgumentNames.return_value = ["batch_size", "enabled"]
    mock_dbutils.widgets.get.side_effect = lambda name: "10" if name == "batch_size" else "true"
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    config = manager.as_config_dict(cast_map={"batch_size": "int", "enabled": "bool"})
    assert config == {"batch_size": 10, "enabled": True}

def test_remove_all_widgets():
    mock_dbutils = MagicMock()
    manager = WidgetManager(dbutils_ref=mock_dbutils)
    manager.remove_all()
    assert mock_dbutils.widgets.remove.call_count > 0

def test_get_config_from_widgets():
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.get.side_effect = lambda name: {
        "container_name": "container",
        "file_pattern": "*.csv",
        "file_encoding": "utf-8",
        "file_delimiter": ",",
        "file_quotechar": "\"",
        "file_escapechar": "\\",
        "skip_lines": "5",
        "audit_table": "audit",
        "sheet_name": "Sheet1",
        "start_cell": "A1"
    }[name]
    config = get_config_from_widgets(mock_dbutils)
    expected_config = ProcessingConfig(
        container_name="container",
        file_pattern="*.csv",
        encoding="utf-8",
        delimiter=",",
        quotechar="\"",
        escapechar="\\",
        skip_lines=5,
        audit_table="audit",
        sheet_name="Sheet1",
        excel_starting_cell="A1"
    )
    assert config == expected_config