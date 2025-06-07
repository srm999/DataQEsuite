import os
import sys
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.data_diffs import normalize_key_value, compare_values


def test_normalize_key_value():
    assert normalize_key_value(' Test_Value ') == 'testvalue'
    assert normalize_key_value(np.nan) == 'NA'
    assert normalize_key_value(None) == 'NA'


def test_compare_values():
    assert compare_values(np.nan, np.nan) is True
    assert compare_values(np.nan, 1) is False
    assert compare_values(5, 5) is True
    assert compare_values('Hello', 'hello') is True
    assert compare_values('Hello', 'world') is False
    assert compare_values(True, True) is True
