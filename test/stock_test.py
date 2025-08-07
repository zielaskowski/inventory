"""stock.py testing"""

import pytest

from app.import_dat import add_stock, bom_import, stock_import
from app.sql import getDF
from conf.config import DEV_ID, DEV_MAN, STOCK_QTY


def _setup_bom_for_commit_test(cli, tmpdir, db_setup):
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


def _setup_stock_for_import(cli, tmpdir, db_setup):
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


def test_commit_project_and_recommit(db_setup, cli, tmpdir):
    """
    Tests that a project can be committed, and that attempting to commit it
    again does change the database state (stock_qty).
    """
    _setup_bom_for_commit_test(cli, tmpdir, db_setup)

    # 1. First Commit
    args = cli.parse_args(["stock", "--add_p", "proj1"])
    add_stock(args)

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
    add_stock(args)

    # 4. Verify State After Second Commit (should be unchanged)
    stock_after_second_commit = getDF(tab="STOCK", follow=True)

    assert (
        stock_after_second_commit.loc[
            stock_after_second_commit[DEV_ID] == "dev1", STOCK_QTY
        ].iloc[0]
        == 20
    )


def test_commit_project_and_use(db_setup, cli, tmpdir):
    """
    Tests that a project can be used, and that attempting to use it
    once normal, second time zero the stock
    """
    _setup_bom_for_commit_test(cli, tmpdir, db_setup)
    _setup_stock_for_import(cli, tmpdir, db_setup)

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


def _setup_bom_for_commit_test2(cli, tmpdir, db_setup):
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


def test_commit_project_and_use_too_much(db_setup, cli, tmpdir, capsys):
    """
    try to use project when not enough stock
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    _setup_stock_for_import(cli, tmpdir, db_setup)

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


def _setup_stock_for_import2(cli, tmpdir, db_setup):
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


def test_missing_project(db_setup, cli, tmpdir, capsys):
    """
    try use project not present in stock
    testing also export
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"

    # 1. use missing project (empty stock)
    args = cli.parse_args(["stock", "--use_pro", "proj1"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "no devices in stock. stock is empty." in out.lower()
    args = cli.parse_args(["stock", "-e"])

    # 15. empty stock table
    args = cli.parse_args(["stock", "-e"])
    with pytest.raises(SystemExit):
        stock_import(args)
    out, _ = capsys.readouterr()
    assert "no data in table stock." in out.lower()

    # 2. use missing project
    _setup_stock_for_import2(cli, tmpdir, db_setup)
    # "device_id,device_manufacturer,stock_qty\n"
    # + "dev1,MAN_A,20\n"
    # + "dev2,MAN_B,40\n"
    args = cli.parse_args(["stock", "--use_pro", "proj2"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "not enough stock for project: ['proj2']" in out.lower()

    # 3. export empty stock
    args = cli.parse_args(["stock", "-e"])
    stock_import(args)
    out, _ = capsys.readouterr()
    assert "20" in out.lower()
    assert "40" in out.lower()
    assert "man_a" in out.lower()
    assert "dev2" in out.lower()


def test_use_one_dev(db_setup, cli, tmpdir, capsys):
    """
    try use project not present in stock
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"
    _setup_stock_for_import2(cli, tmpdir, db_setup)
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


def test_use_one_dev_too_much(db_setup, cli, tmpdir, capsys):
    """
    try use project not present in stock
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"
    _setup_stock_for_import2(cli, tmpdir, db_setup)
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
    stock_after_use = getDF("STOCK", follow=True)
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 19
    )
    out, _ = capsys.readouterr()
    assert "removed device dev1 from man_a" in out.lower()


def test_add_one_dev(db_setup, cli, tmpdir, capsys):
    """
    try use project not present in stock
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"
    _setup_stock_for_import2(cli, tmpdir, db_setup)
    # "device_id,device_manufacturer,stock_qty\n"
    # + "dev1,MAN_A,20\n"
    # + "dev2,MAN_B,40\n"

    # 1. use project
    args = cli.parse_args(
        [
            "stock",
            "--add_device_id",
            "dev1",
            "--add_device_manufacturer",
            "MAN_A",
        ]
    )
    stock_import(args)
    stock_after_use = getDF("STOCK", follow=True)
    assert (
        stock_after_use.loc[stock_after_use[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 21
    )
    out, _ = capsys.readouterr()
    assert "added device dev1 from man_a" in out.lower()


def test_add_device_id_and_manufacturer(db_setup, cli, tmpdir):
    """
    Tests adding a device to stock by its ID and manufacturer.
    """
    _setup_bom_for_commit_test2(cli, tmpdir, db_setup)
    #     "device_id,device_manufacturer,qty,project\n"
    #     + "dev1,MAN_A,40,proj1\n"
    #     + "dev2,MAN_B,20,proj1\n"
    #     + "dev3,MAN_C,30,proj2\n"
    # 1. Add a new device
    args = cli.parse_args(
        [
            "stock",
            "--add_device_id",
            "dev1",
            "--add_device_manufacturer",
            "MAN_A",
        ]
    )
    add_stock(args)

    # 2. Verify the device is in stock with quantity 1
    stock_after_add = getDF(tab="STOCK", follow=True)
    assert len(stock_after_add) == 1
    assert "dev1" in stock_after_add[DEV_ID].to_list()
    assert "MAN_A" in stock_after_add[DEV_MAN].to_list()
    assert (
        stock_after_add.loc[stock_after_add[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 1
    )

    # 3. Add the same device again
    add_stock(args)

    # 4. Verify the stock quantity is now 2
    stock_after_second_add = getDF(tab="STOCK", follow=True)
    assert len(stock_after_second_add) == 1
    assert (
        stock_after_second_add.loc[
            stock_after_second_add[DEV_ID] == "dev1", STOCK_QTY
        ].iloc[0]
        == 2
    )


def test_stock_import_overwrite(db_setup, cli, tmpdir):
    """
    Tests the --overwrite functionality of stock_import.
    """
    # 1. Setup initial stock file
    stock_file_1 = tmpdir.join("stock1.csv")
    with open(stock_file_1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,stock_qty\n"
            + "dev1,MAN_A,10\n"
            + "dev2,MAN_B,20\n"
        )
    args = cli.parse_args(["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv"])
    stock_import(args)

    # 2. Verify initial import
    stock1 = getDF(tab="STOCK", follow=True)
    assert stock1.loc[stock1[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 10
    assert stock1.loc[stock1[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 20

    # 3. Setup second stock file with different quantities
    stock_file_2 = tmpdir.join("stock2.csv")
    with open(stock_file_2, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,stock_qty\n"
            + "dev1,MAN_A,5\n"
            + "dev2,MAN_B,5\n"
        )

    # 4. Import without --overwrite (should add quantities)
    args = cli.parse_args(["stock", "-d", tmpdir.strpath, "-f", "stock2", "-F", "csv"])
    stock_import(args)
    stock2 = getDF(tab="STOCK", follow=True)
    assert stock2.loc[stock2[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 15
    assert stock2.loc[stock2[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 25

    # 5. Import with --overwrite (should overwrite quantities)
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock2", "-F", "csv", "--overwrite"]
    )
    stock_import(args)
    stock3 = getDF(tab="STOCK", follow=True)
    assert stock3.loc[stock3[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 5
    assert stock3.loc[stock3[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 5


def test_stock_import_overwrite_no_existing_data(db_setup, cli, tmpdir):
    """
    Tests the --overwrite functionality when no stock.
    """
    # 1. Setup initial stock file
    stock_file_1 = tmpdir.join("stock1.csv")
    with open(stock_file_1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,stock_qty\n"
            + "dev1,MAN_A,10\n"
            + "dev2,MAN_B,20\n"
        )
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv", "--overwrite"]
    )
    stock_import(args)

    # 2. Verify initial import
    stock1 = getDF(tab="STOCK", follow=True)
    assert stock1.loc[stock1[DEV_ID] == "dev1", STOCK_QTY].iloc[0] == 10
    assert stock1.loc[stock1[DEV_ID] == "dev2", STOCK_QTY].iloc[0] == 20
