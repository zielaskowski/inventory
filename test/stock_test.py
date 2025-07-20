"""stock.py testing"""

import pytest

from app.import_dat import bom_import, commit_project, stock_import
from app.sql import getDF, sql_check
from conf.sql_colnames import DEV_ID, STOCK_QTY
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


def _setup_stock_for_import(cli, tmpdir):
    """Helper to import a BOM with two projects for testing."""
    stock_file = tmpdir.join("stock_commit_test.csv")
    with open(stock_file, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,stock_qty\n"
            + "dev1,MAN_A,20\n"
            + "dev2,MAN_B,40\n"
            + "dev3,MAN_C,30\n"
        )
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock_commit_test", "-F", "csv"]
    )
    stock_import(args)


def test_commit_project_and_recommit(monkeypatch, tmpdir, cli):
    """
    Tests that a project can be committed, and that attempting to commit it
    again does change the database state (stock_qty).
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test(cli, tmpdir)

    # 1. First Commit
    args = cli.parse_args(["stock", "--add_p", "proj1"])
    commit_project(args)

    # 2. Verify State After First Commit
    stock_after_first_commit = getDF(tab="STOCK", follow=True)

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

    assert (
        stock_after_second_commit.loc[
            stock_after_second_commit[DEV_ID] == "dev1", STOCK_QTY
        ].iloc[0]
        == 20
    )


def test_commit_project_and_use(monkeypatch, tmpdir, cli):
    """
    Tests that a project can be used, and that attempting to use it
    once normal, second time zero the stock
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test(cli, tmpdir)
    _setup_stock_for_import(cli, tmpdir)

    # 1. standard use
    args = cli.parse_args(["stock", "--use_pro", "proj1"])
    stock_import(args)

    # 1.1. Verify State After use
    stock_after_use = getDF(tab="STOCK", follow=True)

    # Check that stock table is updated correctly
    assert len(stock_after_use) == 3
    assert "dev1" in stock_after_use[DEV_ID].to_list()
    assert "dev2" in stock_after_use[DEV_ID].to_list()
    assert "dev3" in stock_after_use[DEV_ID].to_list()
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 10
    )
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 20
    )
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev3", STOCK_QTY].iloc[0] == 30
    )
    # 2. use all stock

    args = cli.parse_args(["stock", "--use_pro", "proj1", "proj2"])
    stock_import(args)

    # 2.1. Verify State After use
    stock_after_use = getDF(tab="STOCK", follow=True)

    # Check that stock table is updated correctly
    assert stock_after_use.empty


def _setup_bom_for_commit_test2(cli, tmpdir):
    """Helper to import a BOM with two projects for testing."""
    bom_file = tmpdir.join("bom_commit_test.csv")
    with open(bom_file, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "dev1,MAN_A,40,proj1\n"
            + "dev2,MAN_B,20,proj1\n"
            + "dev3,MAN_C,30,proj2\n"
        )
    args = cli.parse_args(
        ["bom", "-d", tmpdir.strpath, "-f", "bom_commit_test", "-F", "csv"]
    )
    bom_import(args)


def test_commit_project_and_use_too_much(monkeypatch, tmpdir, cli, capsys):
    """
    try to use project when not enough stock
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test2(cli, tmpdir)
    _setup_stock_for_import(cli, tmpdir)

    # 1. use more then have
    args = cli.parse_args(["stock", "--use_pro", "proj1"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "not enough stock for project: ['proj1']" in out.lower()

    # 1.1. Verify State After use
    stock_after_use = getDF(tab="STOCK", follow=True)

    # Check that stock table is updated correctly
    assert len(stock_after_use) == 3
    assert "dev1" in stock_after_use[DEV_ID].to_list()
    assert "dev2" in stock_after_use[DEV_ID].to_list()
    assert "dev3" in stock_after_use[DEV_ID].to_list()
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 20
    )
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 40
    )
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev3", STOCK_QTY].iloc[0] == 30
    )


def _setup_stock_for_import2(cli, tmpdir):
    """Helper to import a BOM with two projects for testing."""
    stock_file = tmpdir.join("stock_commit_test.csv")
    with open(stock_file, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,stock_qty\n"
            + "dev1,MAN_A,20\n"
            + "dev2,MAN_B,40\n"
        )
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock_commit_test", "-F", "csv"]
    )
    stock_import(args)


def test_missing_project(monkeypatch, tmpdir, cli, capsys):
    """
    try use project not present in stock
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test2(cli, tmpdir)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"

    # 1. use missing project (empty stock)
    args = cli.parse_args(["stock", "--use_pro", "proj1"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "no devices in stock. stock is empty." in out.lower()

    # 2. use missing project
    _setup_stock_for_import2(cli, tmpdir)
    # "device_id,device_manufacturer,stock_qty\n"
    # + "dev1,MAN_A,20\n"
    # + "dev2,MAN_B,40\n"
    args = cli.parse_args(["stock", "--use_pro", "proj2"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "not enough stock for project: ['proj2']" in out.lower()


def test_use_one_dev(monkeypatch, tmpdir, cli, capsys):
    """
    try use project not present in stock
    """
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    sql_check()
    _setup_bom_for_commit_test2(cli, tmpdir)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"
    _setup_stock_for_import2(cli, tmpdir)
    # "device_id,device_manufacturer,stock_qty\n"
    # + "dev1,MAN_A,20\n"
    # + "dev2,MAN_B,40\n"

    # 1. use project
    args = cli.parse_args(
        [
            "stock",
            "--use_device_id",
            "dev1",
            "--use_device_manufacturer",
            "MAN_A",
        ]
    )
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "removed device dev1 from man_a" in out.lower()
