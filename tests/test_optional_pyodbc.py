import importlib
import sys
import pytest


def test_modules_handle_missing_pyodbc(monkeypatch):
    original = sys.modules.get('pyodbc')
    monkeypatch.setitem(sys.modules, 'pyodbc', None)

    db_conn = importlib.reload(importlib.import_module('core.db_connection'))
    assert db_conn.pyodbc is None
    with pytest.raises(ImportError):
        db_conn.DatabaseConnection.create_db_connection('s', 'd')

    sql_reader = importlib.reload(importlib.import_module('core.sql_reader'))
    assert sql_reader.pyodbc is None
    reader = sql_reader.SQLFileReader()
    reader.handle_db_exception(Exception('boom'))

    adl_module = importlib.reload(importlib.import_module('refactored.adl_datareader'))
    assert adl_module.pyodbc is None

    if original is not None:
        sys.modules['pyodbc'] = original
        importlib.reload(db_conn)
        importlib.reload(sql_reader)
        importlib.reload(adl_module)
