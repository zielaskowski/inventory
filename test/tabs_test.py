"""
py test
testing functions from app/tabs.py
"""

from unittest.mock import patch

import pandas as pd
import pytest

from app.bom import bom_import
from app.common import tab_cols
from app.error import SqlTabError
from app.sql import getDF, sql_check
from app.tabs import NA_rows
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
