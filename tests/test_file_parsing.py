import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.file_parsing import CSVFileParser


def test_read_csv(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n")

    df = CSVFileParser.read_csv(str(csv_path))
    expected = pd.DataFrame({"a": [1, 3], "b": [2, 4]})
    pd.testing.assert_frame_equal(df, expected)


def test_write_to_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    out_path = tmp_path / "out.csv"
    result = CSVFileParser.write_to_csv(df, str(out_path))
    assert result is True
    read_back = pd.read_csv(out_path)
    pd.testing.assert_frame_equal(read_back, df)


def test_preview_csv(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n5,6\n")

    preview = CSVFileParser.preview_csv(str(csv_path), num_rows=2)
    expected = pd.DataFrame({"a": [1, 3], "b": [2, 4]})
    pd.testing.assert_frame_equal(preview, expected)
