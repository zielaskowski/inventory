"""
py test
testing functions from app/tabs.py
"""

from unittest.mock import mock_open, patch

import pandas as pd
import pytest

from app.common import tab_cols
from app.error import SqlTabError, VimdiffSelError
from app.tabs import ASCII_txt, NA_rows, align_other_cols, vimdiff_selection
from conf.sql_colnames import *
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
            what_differ=DEV_MAN,
            dev_id="",
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


def test_vimdiff_selection_raises_on_length_mismatch():
    """
    Test that vimdiff_selection raises VimdiffSelError if the user adds or removes
    a line, causing the output length to mismatch the input length.
    """

    # 1. Setup: Input data
    ref_col = {"ref": ["a", "b", "c"]}
    change_col = {"change": ["val1", "val2", "val3"]}
    opt_col = {"opt": ["opt1", "opt2", "opt3"]}

    # 2. Simulate user deleting a line in vim
    mock_read_data = "0| val1\n2| val3\n"  # Line 1 is missing

    # 3. Mock external dependencies and expect the exception
    m = mock_open(read_data=mock_read_data)
    with patch("app.tabs.open", m), patch("app.tabs.vimdiff_config"), patch(
        "subprocess.run"
    ), patch("os.remove"):
        with pytest.raises(VimdiffSelError) as excinfo:
            vimdiff_selection(
                ref_col=ref_col,
                change_col=change_col,
                opt_col=opt_col,
                what_differ=DEV_MAN,
                dev_id="",
                exit_on_change=True,
            )
    # 4. Assert that the error message contains the user's incorrect data
    assert "val1" in str(excinfo.value)
    assert "val3" in str(excinfo.value)
    assert "val2" not in str(excinfo.value)


@patch("app.tabs.vimdiff_selection")
@patch("app.tabs.tab_cols")
def test_align_other_cols_user_selects_rm_dat(mock_tab_cols, mock_vimdiff):
    """
    GIVEN rm_dat and keep_dat with different device_description,
    WHEN align_other_cols is called,
    AND user selects the description from rm_dat via vimdiff,
    THEN the returned dataframe contains the description from rm_dat.
    """
    # 1. Setup mocks
    # Mock tab_cols to control the columns being processed
    mock_tab_cols.return_value = (
        [DEV_ID, DEV_MAN],  # must_cols
        [DEV_DESC, DEV_PACK],  # nice_cols
    )

    # Mock vimdiff_selection to simulate user choosing the 'rm_dat' (optional) value
    # The values from rm_dat are passed in the `opt_col` argument.
    # We return those values to simulate the user's choice.
    def vimdiff_side_effect(
        ref_col,
        change_col,
        opt_col,
        what_differ,
        dev_id,
        exit_on_change,
        start_line=1,
    ):
        # The value of the dict is the list of column values to show.
        return next(iter(opt_col.values()))

    mock_vimdiff.side_effect = vimdiff_side_effect

    # 2. Setup test data
    rm_dat = pd.DataFrame(
        {
            "dev_rm": ["device_A"],
            "man_rm": ["manuf_Y"],
            DEV_HASH: ["hash123"],
            DEV_ID: ["device_A"],
            DEV_MAN: ["manuf_Y"],
            DEV_DESC: ["Description from RM"],
            DEV_PACK: ["pckg1"],
        }
    )

    keep_dat = pd.DataFrame(
        {
            DEV_HASH: ["hash123"],
            DEV_ID: ["device_A"],
            DEV_MAN: ["manuf_Y"],
            DEV_DESC: ["Description from KEEP"],
            DEV_PACK: ["pckg1"],
        }
    )

    # 3. Execute the function under test
    result_df = align_other_cols(rm_dat=rm_dat.copy(), keep_dat=keep_dat.copy())

    # 4. Assert the outcome
    assert not result_df.empty
    # Check that the description from rm_dat was chosen
    assert result_df.loc[0, DEV_DESC] == "Description from RM"
    # Check that other columns are untouched
    assert result_df.loc[0, DEV_PACK] == "pckg1"
    assert result_df.loc[0, DEV_HASH] == "hash123"
    assert result_df.loc[0, DEV_MAN] == "manuf_Y"

    # 5. Verify mock was called correctly
    mock_vimdiff.assert_called_once()
    # The call to vimdiff should only contain the differing column
    _, call_kwargs = mock_vimdiff.call_args
    assert call_kwargs["change_col"]["manuf_Y"] == ["Description from KEEP"]
    assert call_kwargs["opt_col"]["manuf_Y"] == ["Description from RM"]


@patch("app.tabs.tab_cols")
def test_align_other_cols_replace_none1(mock_tab_cols):
    """
    GIVEN rm_dat and keep_dat with different device_description,
    incoming data with None in description
    WHEN align_other_cols is called,
    AND user selects the description from rm_dat via vimdiff,
    THEN the returned dataframe contains the description from rm_dat.
    automatically fill None in description, without user interraction.
    """
    # 1. Setup mocks
    # Mock tab_cols to control the columns being processed
    mock_tab_cols.return_value = (
        [DEV_ID, DEV_MAN],  # must_cols
        [DEV_DESC, DEV_PACK],  # nice_cols
    )

    # 2. Setup test data
    rm_dat = pd.DataFrame(
        {
            "dev_rm": ["device_A"],
            "man_rm": ["manuf_Y"],
            DEV_HASH: ["hash123"],
            DEV_ID: ["device_A"],
            DEV_MAN: ["manuf_Y"],
            DEV_DESC: ["Description from RM"],
            DEV_PACK: ["pckg1"],
        }
    )

    keep_dat = pd.DataFrame(
        {
            DEV_HASH: ["hash123"],
            DEV_ID: ["device_A"],
            DEV_MAN: ["manuf_Y"],
            DEV_DESC: [None],
            DEV_PACK: ["pckg1"],
        }
    )

    # 3. Execute the function under test
    result_df = align_other_cols(rm_dat=rm_dat.copy(), keep_dat=keep_dat.copy())

    # 4. Assert the outcome
    assert not result_df.empty
    # Check that the description from rm_dat was chosen
    assert result_df.loc[0, DEV_DESC] == "Description from RM"
    # Check that other columns are untouched
    assert result_df.loc[0, DEV_PACK] == "pckg1"
    assert result_df.loc[0, DEV_HASH] == "hash123"
    assert result_df.loc[0, DEV_MAN] == "manuf_Y"
