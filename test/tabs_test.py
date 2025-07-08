"""
py test
testing functions from app/tabs.py
"""

from unittest.mock import mock_open, patch

import pandas as pd
import pytest

from app.common import tab_cols
from app.error import SqlTabError
from app.tabs import ASCII_txt, NA_rows, vimdiff_selection
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


def test_vimdiff_selection_handles_none_input():
    """
    Test that vimdiff_selection correctly handles a None value in its input list,
    by converting it to the string 'None' without crashing.
    """

    # 1. Setup: Input data with a None value
    ref_col = {"ref": ["a", "b", "c"]}
    change_col = {"change": ["val1", None, "val3"]}
    opt_col = {"opt": ["opt1", "opt2", "opt3"]}

    # This is what the user "saves" in vim. We simulate them changing the 'None' line.
    mock_read_data = "0| val1\n1| new_val2\n2| val3\n"

    # 2. Mock all external dependencies (file I/O and subprocess)
    m = mock_open(read_data=mock_read_data)
    with patch("app.tabs.open", m), patch("app.tabs.vimdiff_config"), patch(
        "subprocess.run"
    ), patch("os.remove"):
        # 3. Call the function
        result = vimdiff_selection(
            ref_col=ref_col,
            change_col=change_col,
            opt_col=opt_col,
            exit_on_change=True,
        )

    # 4. Assertions
    # Assert that the function returned the list we specified in mock_read_data
    assert result == ["val1", "new_val2", "val3"]

    # Find the call that wrote the file for 'change_col' and check its content.
    # We iterate through all write calls made by the mock_open object.
    found_none_write = False
    for call in m().write.call_args_list:
        written_string = call.args[0]
        # The line with None should have been written as '1| None\n'
        if written_string == "1| None\n":
            found_none_write = True
            break

    assert (
        found_none_write
    ), "A 'None' value was not correctly written to the temp file."
