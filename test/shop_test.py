"""SHOP import testing"""

from unittest.mock import patch

import pandas as pd
import pytest

from app.common import DEV_ID, DEV_MAN, read_json_dict
from app.shop import shop_import
from app.sql import getDF, put, sql_check
from app.tabs import align_column_duplications, columns_align, foreign_tabs, prepare_tab
from conf.config import SQL_SCHEME
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_shop_import_csv1(monkeypatch, tmpdir, cli):
    """1. import from csv"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()

    test = tmpdir.join("test.csv")
    with open(test, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price\n"
            + "aa,bb,1,10"
            ) # fmt:skip

    inp = pd.read_csv(test)
    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-F", "csv"])
    shop_import(args)
    exp = getDF(tab="SHOP")
    common_cols = exp.columns.intersection(inp.columns)
    pd.testing.assert_frame_equal(
        exp[common_cols],
        inp[common_cols],
        check_dtype=False,
    )


def test_shop_import_csv2(monkeypatch, tmpdir, cli):
    """2. import again with new date (add)"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    test = tmpdir.join("test.csv")
    with open(test, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price\n"
            + "aa,bb,1,10"
        ) # fmt: skip
    inp = pd.read_csv(test)
    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-F", "csv"])
    dat = columns_align(
        inp.copy(),
        file=test.strpath,
        args=args,
    )
    dat = prepare_tab(
        dat=dat.copy(),
        tab="SHOP",
        file=test.strpath,
        row_shift=10,
    )
    dat["date"] = "2025-06-01"
    sql_scheme = read_json_dict(SQL_SCHEME)
    for tab in foreign_tabs("SHOP") + ["SHOP"]:
        put(
            dat=dat,
            tab=tab,
            on_conflict=sql_scheme[tab].get("ON_CONFLICT", {}),
        )
    shop_import(args)
    exp = getDF(tab="SHOP")
    exp["date"] = exp["date"].apply(str)
    dat["date"] = dat["date"].apply(str)
    common_cols = exp.columns.intersection(dat.columns)
    exp = exp.loc[exp["date"] == dat.loc[0, "date"], common_cols]
    pd.testing.assert_frame_equal(
        exp[common_cols],
        dat[common_cols],
        check_dtype=False,
    )


def test_shop_import_csv3(monkeypatch, tmpdir, cli):
    """3. import again with the same date"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()

    test = tmpdir.join("test.csv")
    with open(test, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price\n"
            + "aa,bb,1,10"
            ) # fmt: skip

    inp = pd.read_csv(test)
    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-F", "csv"])
    shop_import(args)
    shop_import(args)
    exp = getDF(tab="SHOP")
    common_cols = exp.columns.intersection(inp.columns)
    pd.testing.assert_frame_equal(
        exp[common_cols],
        inp[common_cols],
        check_dtype=False,
    )


def test_shop_import_csv4(monkeypatch, tmpdir, cli, capsys):
    """4. info about columns"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    args = cli.parse_args(["shop", "--info"])
    shop_import(args)
    out, _ = capsys.readouterr()
    exp_out = [
        "these columns must be present in import file:",
        "price",
        "device_id",
        "device_manufacturer",
        "order_qty",
    ]
    for t in exp_out:
        assert t in out.lower()


def test_shop_import_csv6(monkeypatch, tmpdir, cli):
    """6. the same device_id but different manufacturer"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    monkeypatch.setattr("app.common.DEBUG", True)
    sql_check()

    test1 = tmpdir.join("test1.csv")
    with open(test1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price\n"
            + "aa,aa,1,10\n"
            + "aa,ab,1,10"
        )

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-F", "csv"])
    shop_import(args)

    test2 = tmpdir.join("test2.csv")
    with open(test2, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price\n"
            + "aa,ac,1,10"
            )# fmt: skip

    inp = pd.read_csv(test2)
    alternatives = {DEV_MAN: ["ac"], DEV_MAN + "_stock": ["aa | ab"]}
    with patch("app.tabs.select_column") as mock_select_column:
        align_column_duplications(inp, merge_on=DEV_ID, duplication=DEV_MAN)
        mock_select_column.assert_called_with(alternatives=alternatives, column=DEV_MAN)


def test_shop_import_csv7(monkeypatch, tmpdir, cli):
    """6. the same device_id but different description"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    monkeypatch.setattr("app.tabs.DEBUG", True)
    sql_check()

    test1 = tmpdir.join("test1.csv")
    with open(test1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,device_description,order_qty,price\n"
            + "aa,aa,desc1,1,10\n"
            + "aa,ab,desc2,1,10"
        )
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    shop_import(args)

    test2 = tmpdir.join("test2.csv")
    with open(test2, "w", encoding="UTF8") as f:
        f.write("device_id,device_manufacturer,device_description,order_qty,price\n")
        f.write("aa,aa,desc3,1,10")

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-F", "csv", "-f", "test2"])
    shop_import(args)

    test3 = tmpdir.join("test3.csv")
    with open(test3, "w", encoding="UTF8") as f:
        f.write("device_id,device_manufacturer,device_description,order_qty,price\n")
        f.write("aa,ab,desc2,1,10\n")
        f.write("aa,aa,desc3,1,10")

    inp = pd.read_csv(test3)
    exp = getDF(tab="DEVICE")
    common_cols = exp.columns.intersection(inp.columns).to_list()
    exp = exp.sort_values(by=common_cols).reset_index()
    inp = inp.sort_values(by=common_cols).reset_index()
    pd.testing.assert_frame_equal(
        exp[common_cols],
        inp[common_cols],
        check_dtype=False,
    )
