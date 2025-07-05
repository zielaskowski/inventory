"""
py test
testing functions from app/tabs.py
"""

import pandas as pd
import pytest

from app.common import tab_cols
from app.error import SqlTabError
from app.tabs import ASCII_txt, NA_rows
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_NA_rows(capsys):
    """NA rows in must and nice cols"""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "must": [1, 1, 10, None, 30, 40],
            "nice": ["a", "a", "a", "b", None, "d"],
        }
    )
    expected_ids = [1, 2, 3, 5, 6]
    df = NA_rows(df, must_cols=["must"], nice_cols=["nice"], row_shift=0)

    assert list(df["id"]) == expected_ids

    # Capture stdout and verify that info was printed for must_col NA
    out, _ = capsys.readouterr()
    assert "[5]" in out.lower()
    assert "30.0" in out.lower()
    assert "none" in out.lower()


def test_NA_rows1(capsys):
    """NA rows only in nice col"""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "must": [1, 1, 10, 11, 30, 40],
            "nice": ["a", "a", "a", "b", None, "d"],
        }
    )
    expected_ids = [1, 2, 3, 4, 5, 6]
    df = NA_rows(df, must_cols=["must"], nice_cols=["nice"], row_shift=0)

    assert list(df["id"]) == expected_ids

    # Capture stdout and verify that info was printed for must_col NA
    out, _ = capsys.readouterr()
    assert "5" in out.lower()
    assert "30" in out.lower()
    assert "none" in out.lower()


def test_tab_cols1(monkeypatch, tmpdir):
    """wrong table name"""
    scheme = '{"tab1": {"col1": "TEXT"}}'
    f = tmpdir.join("shceme.json")
    f.write(scheme)
    monkeypatch.setattr("conf.config.SQL_SCHEME", f)
    with pytest.raises(SqlTabError) as err_info:
        tab_cols("test")
    assert err_info.match("test")


def test_tab_cols2(monkeypatch, tmpdir):
    """expected behaviour"""
    scheme = '{"tab1": {"col1": "TEXT"}, "tab2": {"col2": "TEXT"}}'
    f = tmpdir.join("shceme.json")
    f.write(scheme)
    monkeypatch.setattr("conf.config.SQL_SCHEME", f)
    with pytest.raises(SqlTabError) as err_info:
        tab_cols("test")
    assert err_info.match("test")


def test_ASCII_txt_ohm_conversion():
    """Test that ASCII_txt correctly converts 'Ω' to 'ohm'."""

    # Test with the Omega symbol
    assert ASCII_txt("100Ω") == "100ohm"
    # Test with other non-ASCII characters
    assert ASCII_txt("resistor 100Ω (示例文本)") == "resistor 100ohm "
    # Test with no Omega symbol
    assert ASCII_txt("standard resistor") == "standard resistor"
    # Test with only the Omega symbol
    assert ASCII_txt("Ω") == "ohm"
    # Test with None
    assert ASCII_txt(None) is None
    # Test with NaN
    assert ASCII_txt(pd.NA) is pd.NA
