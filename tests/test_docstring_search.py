import core.docstring_search as ds


def test_search_read_csv_docstring():
    results = ds.search_docstrings("read a csv file", ["core.file_parsing"])
    assert any("CSVFileParser.read_csv" in name for name in results)
