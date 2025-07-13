"""stock.py testing"""

import pandas as pd
import pytest

from app.common import BOM_COMMITTED, DEV_ID, STOCK_QTY
from app.import_dat import bom_import, commit_project
from app.sql import getDF, sql_check
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def _setup_bom_for_commit_test(cli, tmpdir):
    """Helper to import a BOM with two projects for testing."""
    bom_file = tmpdir.join("bom_commit_test.csv")
    with open(bom_file, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "dev1,MAN_A,10,proj1\n"
            + "dev2,MAN_B,20,proj1\n"
            + "dev3,MAN_C,30,proj2\n"
        )
    args = cli.parse_args(
        ["bom", "-d", tmpdir.strpath, "-f", "bom_commit_test", "-F", "csv"]
    )
    bom_import(args)


def test_commit_project_and_recommit(monkeypatch, tmpdir, cli):
    """
    Tests that a project can be committed, and that attempting to commit it
    again does not change the database state.
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test(cli, tmpdir)

    # 1. First Commit
    args = cli.parse_args(["stock", "-p", "proj1"])
    commit_project(args)

    # 2. Verify State After First Commit
    bom_after_first_commit = getDF(tab="BOM", follow=True)
    stock_after_first_commit = getDF(tab="STOCK", follow=True)

    # Check that proj1 is marked as committed
    proj1_bom = bom_after_first_commit[bom_after_first_commit["project"] == "proj1"]
    assert proj1_bom.loc[:, BOM_COMMITTED].all()

    # Check that proj2 is NOT marked as committed
    proj2_bom = bom_after_first_commit[bom_after_first_commit["project"] == "proj2"]
    assert not proj2_bom.loc[:, BOM_COMMITTED].any()

    # Check that stock table is updated correctly
    assert len(stock_after_first_commit) == 2
    assert "dev1" in stock_after_first_commit[DEV_ID].to_list()
    assert "dev2" in stock_after_first_commit[DEV_ID].to_list()
    assert (
        stock_after_first_commit.loc[
            stock_after_first_commit[DEV_ID] == "dev1", STOCK_QTY
        ].iloc[0]
        == 10
    )

    # 3. Second (Duplicate) Commit
    commit_project(args)

    # 4. Verify State After Second Commit (should be unchanged)
    stock_after_second_commit = getDF(tab="STOCK", follow=True)
    bom_after_second_commit = getDF(tab="BOM", follow=True)

    # Check that stock quantities have NOT been incremented again
    assert stock_after_second_commit.equals(stock_after_first_commit)
    assert bom_after_second_commit.equals(bom_after_first_commit)
