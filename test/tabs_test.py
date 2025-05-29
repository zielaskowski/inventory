"""
py test
testing functions from app/tabs.py
"""

import pandas as pd
import pytest

from app.tabs import NA_rows


def test_NA_rows(capfd):
    # example data
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
    out, _ = capfd.readouterr()
    assert "[5]" in out.lower()
    assert "30.0" in out.lower()
    assert "none" in out.lower()
