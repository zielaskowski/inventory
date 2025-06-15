"""pytest units for transaction module"""

from unittest.mock import patch

import pandas as pd
import pytest

from app.bom import bom_import
from app.shop import shop_import
from app.sql import sql_check
from app.transaction import trans
from conf.config import DISP_CURR
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_trans1(monkeypatch, cli, tmpdir):
    """
    standard transaction or selected project with split on shops
    also consider min qty and price calculation
    """
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    bom = tmpdir.join("bom.csv")
    with open(bom, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,aa,1,test\n"
            + "bb,bb,9,test\n"
            + "cc,cc,1,test\n"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom", "-F", "csv"])
    bom_import(args)

    shop = tmpdir.join("shop.csv")
    with open(shop, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price,shop\n"
            + "aa,aa,5,10,pytest1\n"
            + "aa,aa,10,20,pytest2\n"
            + "bb,bb,10,20,pytest1\n"
            + "bb,bb,8,10,pytest2\n"
            + "cc,cc,10,20,pytest1\n"
            + "cc,cc,30,10,pytest2\n"
            ) # fmt:skip

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-f", "shop", "-F", "csv"])
    shop_import(args)

    args = cli.parse_args(["trans", "-d", tmpdir.strpath, "-f", "pytest"])
    with patch("app.transaction.msg.trans_summary") as mock_trans_summary:
        trans(args)
        mock_arg, _ = mock_trans_summary.call_args

    imp1_f = tmpdir.join("imp1.csv")
    with open(imp1_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "5,aa,aa,,pytest1,-\n"
            + "10,cc,cc,,pytest1,-\n"
        )

    imp2_f = tmpdir.join("imp2.csv")
    with open(imp2_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "16,bb,bb,,pytest2,-"
        )

    exp1 = pd.read_csv(tmpdir.strpath + "/pytest_pytest1.csv")
    exp2 = pd.read_csv(tmpdir.strpath + "/pytest_pytest2.csv")
    imp1 = pd.read_csv(imp1_f)
    imp2 = pd.read_csv(imp2_f)
    pd.testing.assert_frame_equal(exp1, imp1, check_dtype=False)
    pd.testing.assert_frame_equal(exp2, imp2, check_dtype=False)
    assert mock_arg[0][0]["price"] == str(5 * 10 + 10 * 20) + DISP_CURR  # shop: pytest1
    assert mock_arg[0][1]["price"] == str(16 * 10) + DISP_CURR  # shop: pytest2


def test_trans2(monkeypatch, cli, tmpdir):
    """standard transaction or selected project without split on shops"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    bom = tmpdir.join("bom.csv")
    with open(bom, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,aa,1,test\n"
            + "bb,bb,9,test\n"
            + "cc,cc,1,test\n"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom", "-F", "csv"])
    bom_import(args)

    shop = tmpdir.join("shop.csv")
    with open(shop, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price,shop\n"
            + "aa,aa,5,10,pytest1\n"
            + "aa,aa,10,20,pytest2\n"
            + "bb,bb,10,20,pytest1\n"
            + "bb,bb,8,10,pytest2\n"
            + "cc,cc,10,20,pytest1\n"
            + "cc,cc,30,10,pytest2\n"
            ) # fmt:skip

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-f", "shop", "-F", "csv"])
    shop_import(args)

    args = cli.parse_args(["trans", "-d", tmpdir.strpath, "-f", "pytest", "-s"])
    with patch("app.transaction.msg.trans_summary") as mock_trans_summary:
        trans(args)
        mock_arg, _ = mock_trans_summary.call_args

    imp1_f = tmpdir.join("imp1.csv")
    with open(imp1_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "1,aa,aa,,any,-\n"
            + "9,bb,bb,,any,-\n"
            + "1,cc,cc,,any,-\n"
        )

    exp1 = pd.read_csv(tmpdir.strpath + "/pytest_any.csv")
    imp1 = pd.read_csv(imp1_f)
    pd.testing.assert_frame_equal(exp1, imp1, check_dtype=False)
    assert mock_arg[0][0]["price"] == "-"  # shop: pytest1


def test_trans3(monkeypatch, cli, tmpdir):
    """
    multiply qty
    standard transaction or selected project with split on shops
    also consider min qty and price calculation
    """
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    bom = tmpdir.join("bom.csv")
    with open(bom, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,aa,1,test\n"
            + "bb,bb,9,test\n"
            + "cc,cc,1,test\n"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom", "-F", "csv"])
    bom_import(args)

    shop = tmpdir.join("shop.csv")
    with open(shop, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price,shop\n"
            + "aa,aa,5,10,pytest1\n"
            + "aa,aa,10,20,pytest2\n"
            + "bb,bb,10,20,pytest1\n"
            + "bb,bb,8,10,pytest2\n"
            + "cc,cc,10,20,pytest1\n"
            + "cc,cc,30,10,pytest2\n"
            ) # fmt:skip

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-f", "shop", "-F", "csv"])
    shop_import(args)

    args = cli.parse_args(["trans", "-d", tmpdir.strpath, "-f", "pytest", "-q", "10"])
    with patch("app.transaction.msg.trans_summary") as mock_trans_summary:
        trans(args)
        mock_arg, _ = mock_trans_summary.call_args

    imp1_f = tmpdir.join("imp1.csv")
    with open(imp1_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "10,aa,aa,,pytest1,-\n"
            + "10,cc,cc,,pytest1,-\n"
        )

    imp2_f = tmpdir.join("imp2.csv")
    with open(imp2_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "96,bb,bb,,pytest2,-"
        )

    exp1 = pd.read_csv(tmpdir.strpath + "/pytest_pytest1.csv")
    exp2 = pd.read_csv(tmpdir.strpath + "/pytest_pytest2.csv")
    imp1 = pd.read_csv(imp1_f)
    imp2 = pd.read_csv(imp2_f)
    pd.testing.assert_frame_equal(exp1, imp1, check_dtype=False)
    pd.testing.assert_frame_equal(exp2, imp2, check_dtype=False)
    assert (
        mock_arg[0][0]["price"] == str(10 * 10 + 10 * 20) + DISP_CURR
    )  # shop: pytest1
    assert mock_arg[0][1]["price"] == str(96 * 10) + DISP_CURR  # shop: pytest2


def test_trans4(monkeypatch, cli, tmpdir):
    """
    select only one project
    standard transaction or selected project with split on shops
    also consider min qty and price calculation
    """
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    bom = tmpdir.join("bom.csv")
    with open(bom, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,aa,1,test\n"
            + "bb,bb,9,test\n"
            + "cc,cc,1,test2\n"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom", "-F", "csv"])
    bom_import(args)

    shop = tmpdir.join("shop.csv")
    with open(shop, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,order_qty,price,shop\n"
            + "aa,aa,5,10,pytest1\n"
            + "aa,aa,10,20,pytest2\n"
            + "bb,bb,10,20,pytest1\n"
            + "bb,bb,8,10,pytest2\n"
            + "cc,cc,10,20,pytest1\n"
            + "cc,cc,30,10,pytest2\n"
            ) # fmt:skip

    args = cli.parse_args(["shop", "-d", tmpdir.strpath, "-f", "shop", "-F", "csv"])
    shop_import(args)

    args = cli.parse_args(["trans", "-d", tmpdir.strpath, "-f", "pytest", "-p", "test"])
    with patch("app.transaction.msg.trans_summary") as mock_trans_summary:
        trans(args)
        mock_arg, _ = mock_trans_summary.call_args

    imp1_f = tmpdir.join("imp1.csv")
    with open(imp1_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "5,aa,aa,,pytest1,-\n"
        )

    imp2_f = tmpdir.join("imp2.csv")
    with open(imp2_f, "w", encoding="UTF8") as f:
        f.write(
            "order_qty,device_id,device_manufacturer,device_description,shop,shop_id\n"
            + "16,bb,bb,,pytest2,-"
        )

    exp1 = pd.read_csv(tmpdir.strpath + "/pytest_pytest1.csv")
    exp2 = pd.read_csv(tmpdir.strpath + "/pytest_pytest2.csv")
    imp1 = pd.read_csv(imp1_f)
    imp2 = pd.read_csv(imp2_f)
    pd.testing.assert_frame_equal(exp1, imp1, check_dtype=False)
    pd.testing.assert_frame_equal(exp2, imp2, check_dtype=False)
    assert mock_arg[0][0]["price"] == str(5.0 * 10) + DISP_CURR  # shop: pytest1
    assert mock_arg[0][1]["price"] == str(16.0 * 10) + DISP_CURR  # shop: pytest2
