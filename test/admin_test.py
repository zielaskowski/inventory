"""pytest units"""

from unittest.mock import patch

import pandas as pd
import pytest

from app.admin import align
from app.bom import bom_import
from app.shop import shop_import
from app.sql import getDF, sql_check
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_align_man1(monkeypatch, tmpdir, cli):
    """
    basic test
    change device manufacturer to other (already existed)
    expected added qty.
    change description to existing one
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

    # base data
    bom1 = tmpdir.join("bom1.csv")
    with open(bom1, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description\n"
        + "da,maa,1,desc11\n"
        + "da,mab,1,desc12\n"
        + "db,mbb,9,desc21\n"
        + "db,mbc,9,desc22\n"
        + "db,mcc,1,desc33\n"
        + "dc,mcc,1,desc34\n"
        )# fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom1", "-F", "csv"])
    bom_import(args)

    with patch(
        "app.tabs.vimdiff_selection",
        side_effect=[
            ["maa", "mab", "mbc", "mbc", "mcc"],
            ["maa", "mab", "mbc", "mbc", "mcc"],
            ["desc22"],
        ],
    ):
        align()

    dev = getDF(tab="BOM", follow=True)

    # expected data
    expect_dat = tmpdir.join("expect_dat.csv")
    with open(expect_dat, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description\n"
        + "da,maa,1,desc11\n"
        + "da,mab,1,desc12\n"
        + "db,mbc,18,desc22\n"
        + "db,mcc,1,desc33\n"
        + "dc,mcc,1,desc34\n"
        )# fmt: skip
    exp_dat = pd.read_csv(expect_dat)
    dev = dev.reindex(columns=exp_dat.columns)
    dev.sort_values(
        by=["device_id", "device_manufacturer"],
        inplace=True,
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(dev, exp_dat, check_dtype=False)


def test_align_man2(monkeypatch, tmpdir, cli):
    """
    multiple projects in BOM
    and then add existing device in SHOP
    importing device and manufacturer already present but changing manuf to other
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

    # base data
    bom1 = tmpdir.join("bom1.csv")
    with open(bom1, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project\n"
        + "da,maa,1,desc11,proj1\n"
        + "da,mab,1,desc12,proj2\n"
        + "db,mbb,9,desc21,proj2\n"
        + "db,mbb,9,desc22,proj1\n"
        + "db,mcc,1,desc33,proj2\n"
        + "dc,mcc,1,desc34,proj2\n"
        )# fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom1", "-F", "csv"])
    bom_import(args)

    with patch(
        "app.tabs.vimdiff_selection",
        side_effect=[
            ["maa", "maa", "mbb", "mbb"],
            ["maa", "maa", "mbb", "mbb"],
            ["desc11"],
            ["desc11"],
        ],
    ):
        align()

    dev = getDF(tab="BOM", follow=True)

    # expected data
    expect_dat = tmpdir.join("expect_dat.csv")
    with open(expect_dat, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project\n"
        + "da,maa,1,desc11,proj1\n"
        + "da,maa,1,desc11,proj2\n"
        + "db,mbb,10,desc11,proj2\n"
        + "db,mbb,9,desc11,proj1\n"
        + "dc,mcc,1,desc34,proj2\n"
        )# fmt: skip
    exp_dat = pd.read_csv(expect_dat)
    dev = dev.reindex(columns=exp_dat.columns)
    dev.sort_values(
        by=["device_id", "device_manufacturer"],
        inplace=True,
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(dev, exp_dat, check_dtype=False)


def test_align_man4(monkeypatch, tmpdir, cli):
    """
    test ffill: multiple projects in BOM
    and then add existing device in SHOP
    importing device and manufacturer already present but changing manuf to other
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

    # base data
    bom1 = tmpdir.join("bom1.csv")
    with open(bom1, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project\n"
        + "da,maa,1,desc11,proj1\n"
        + "da,mab,1,desc12,proj2\n"
        + "db,mbb,9,desc21,proj2\n"
        + "db,mbb,9,desc22,proj1\n"
        + "db,mcc,1,desc33,proj2\n"
        + "dc,mcc,1,desc34,proj2\n"
        )# fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom1", "-F", "csv"])
    bom_import(args)

    with patch(
        "app.tabs.vimdiff_selection",
        side_effect=[
            ["maa", "mab", "mcc", "mcc"],
            ["maa", "mab", "mcc", "mcc"],
            ["desc11"],
        ],
    ):
        align()

    dev = getDF(tab="BOM", follow=True)

    # expected data
    expect_dat = tmpdir.join("expect_dat.csv")
    with open(expect_dat, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project\n"
        + "da,maa,1,desc11,proj1\n"
        + "da,mab,1,desc12,proj2\n"
        + "db,mcc,9,desc11,proj1\n"
        + "db,mcc,10,desc11,proj2\n"
        + "dc,mcc,1,desc34,proj2\n"
        )# fmt: skip
    exp_dat = pd.read_csv(expect_dat)
    dev = dev.reindex(columns=exp_dat.columns)
    dev.sort_values(
        by=["device_id", "device_manufacturer"],
        inplace=True,
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(dev, exp_dat, check_dtype=False)


def test_align_man5(monkeypatch, tmpdir, cli):
    """
    multiple projects in BOM, some attributes for devices
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

    # base data
    bom1 = tmpdir.join("bom1.csv")
    with open(bom1, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project,dev_category1,dev_category2,package\n"
        + "da,maa,1,desc11,proj1,cat1,cat2,pack1\n"
        + "da,mab,1,desc12,proj2,cat1,cat2,pack1\n"
        + "db,mbb,9,desc21,proj2,cat1,cat2,pack1\n"
        + "db,mbb,9,desc22,proj1,cat1,cat2,pack1\n"
        + "db,mcc,1,desc33,proj2,cat3,cat4,pack5\n"
        + "dc,mcc,1,desc34,proj2,cat1,cat2,pack1\n"
        )# fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom1", "-F", "csv"])
    bom_import(args)

    with patch(
        "app.tabs.vimdiff_selection",
        side_effect=[
            ["maa", "mab", "mcc", "mcc"],
            ["maa", "mab", "mcc", "mcc"],
            ["cat3", "cat4", "desc11", "pack5"],
        ],
    ):
        align()

    dev = getDF(tab="BOM", follow=True)

    # expected data
    expect_dat = tmpdir.join("expect_dat.csv")
    with open(expect_dat, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description,project,dev_category1,dev_category2,package\n"
        + "da,maa,1,desc11,proj1,cat1,cat2,pack1\n"
        + "da,mab,1,desc12,proj2,cat1,cat2,pack1\n"
        + "db,mcc,9,desc11,proj1,cat3,cat4,pack5\n"
        + "db,mcc,10,desc11,proj2,cat3,cat4,pack5\n"
        + "dc,mcc,1,desc34,proj2,cat1,cat2,pack1\n"
        )# fmt: skip
    exp_dat = pd.read_csv(expect_dat)
    dev = dev.reindex(columns=exp_dat.columns)
    dev.sort_values(
        by=["device_id", "device_manufacturer"],
        inplace=True,
        ignore_index=True,
    )
    # assert all(exp_dat.apply(sorted,axis='rows') == dev.apply(sorted,axis='rows'))
    pd.testing.assert_frame_equal(dev, exp_dat, check_dtype=False)


# keep empty lines in opt col to keep alignment


# tfest error when user deleted/added lines
