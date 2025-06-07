import os
import sys
import pandas as pd
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.data_processing import (
    convert_bytearray_columns,
    remove_timezone_info,
    fill_nulls_and_blanks,
    strip_string_values,
    standardize_column_names,
)


def test_convert_bytearray_columns():
    df = pd.DataFrame({
        'A': [bytearray(b'abc'), bytearray(b'def'), 'ghi'],
        'B': [1, 2, 3],
    })
    result = convert_bytearray_columns(df.copy())
    expected = pd.DataFrame({'A': ['abc', 'def', 'ghi'], 'B': [1, 2, 3]})
    assert_frame_equal(result, expected)


def test_remove_timezone_info():
    df = pd.DataFrame({
        'date': [pd.Timestamp('2021-01-01', tz='UTC'), pd.Timestamp('2021-01-02', tz='UTC')],
        'value': [1, 2],
    })
    result = remove_timezone_info(df.copy())
    expected = pd.DataFrame({
        'date': [pd.Timestamp('2021-01-01'), pd.Timestamp('2021-01-02')],
        'value': [1, 2],
    })
    assert_frame_equal(result, expected)
    assert result['date'].dt.tz is None


def test_fill_nulls_and_blanks():
    df = pd.DataFrame({
        'num': [1, float('nan'), 3],
        'text': ['a', '', None],
        'flag': [True, None, False],
    })
    result = fill_nulls_and_blanks(df.copy(), fill_value=0)
    expected = pd.DataFrame({
        'num': [1.0, 0.0, 3.0],
        'text': ['a', '0', '0'],
        'flag': [True, '0', False],
    })
    assert_frame_equal(result.reset_index(drop=True), expected)


def test_strip_string_values():
    df = pd.DataFrame({'A': ['  hi ', ' there '], 'B': [1, 2]})
    result = strip_string_values(df.copy())
    expected = pd.DataFrame({'A': ['hi', 'there'], 'B': [1, 2]})
    assert_frame_equal(result, expected)


def test_standardize_column_names():
    df = pd.DataFrame({' col_one ': [1], 'colTwo ': [2], 'COL_THREE': [3]})
    result = standardize_column_names(df.copy())
    assert result.columns.tolist() == ['COL ONE', 'COLTWO', 'COL THREE']
