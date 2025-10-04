"""BOM.py testing"""

import pandas as pd
import pytest

from app.import_dat import bom_import, scan_files
from app.sql import getDF, sql_check
from conf.config import BOM_QTY, DEV_ID
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_bom_no_dir(cli, capsys):
    """no directory to import from"""
    with pytest.raises(SystemExit):
        cli.parse_args(["bom"])
    out, _ = capsys.readouterr()
    assert "bom_import" in out.lower()


def test_bom_no_args(cli, capsys):
    """no files found"""
    args = cli.parse_args(["bom", "-d", "."])
    with pytest.raises(SystemExit) as err_info:
        bom_import(args)
    out, _ = capsys.readouterr()
    assert "no files found" in out.lower()
    assert err_info.value.code == 1


def test_bom_empty_sql(cli, capsys, db_setup):  # pylint: disable=unused-argument
    """empty sql so no files to reimport"""
    args = cli.parse_args(["bom", "-r"])
    with pytest.raises(SystemExit) as err_info:
        bom_import(args)
    out, _ = capsys.readouterr()
    assert "reimport" in out.lower()
    assert err_info.value.code == 1


def test_bom_no_permission(cli):
    """scaninig folder without permission"""
    args = cli.parse_args(["bom", "-d", "/"])
    with pytest.raises(SystemExit) as err_info:
        bom_import(args)
    assert err_info.value.code == 1


def test_bom_import_csv(cli, db_setup, tmpdir):
    """import and export without errors"""
    test = tmpdir.join("test.csv")
    with open(test, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,bb,1,test"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    inp = pd.read_csv(test)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv11(cli, db_setup, tmpdir):
    """import and export without errors, strip text during import"""
    test = tmpdir.join("test.csv")
    with open(test, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + " aa , bb ,1,test"
        )# fmt: skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    inp = pd.read_csv(test).apply(lambda x: x.str.strip() if x.dtype == object else x)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_export(cli, db_setup, tmpdir):
    """export with not existing hidden columns"""
    csv = tmpdir.join("test.csv")
    csv.write("device_id,device_manufacturer,qty,project\n")
    csv.write("aa,bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    args = cli.parse_args(
        [
            "bom",
            "-d",
            tmpdir.strpath,
            "-f",
            "exp.csv",
            "-e",
            "%",
            "--export_columns",
            "device_id",
        ]
    )
    bom_import(args)
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv1(cli, db_setup, tmpdir, capsys):
    """import empty csv file
    error: no header in file"""
    csv = tmpdir.join("test.csv")
    csv.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "unexpected error" in out.lower()


def test_bom_import_csv2(cli, db_setup, tmpdir, capsys):
    """missing mandatory columns"""
    csv = tmpdir.join("test.csv")
    csv.write("device_manufacturer,qty,project\n")
    csv.write("bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "missing columns: ['device_id']" in out.lower()


def test_bom_import_csv3(cli, db_setup, tmpdir, capsys):
    """NAs in mandatory rows"""
    sql_check()
    csv = tmpdir.join("test.csv")
    with open(csv, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n"
            + "aa,bb,1,test\n"
            + ",bb,1,test\n"
            + "aa,,1,test\n"
            + "aa,bb,1,test\n"
        )
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)

    out, _ = capsys.readouterr()
    assert "missing necessery" in out.lower()

    exp = tmpdir.join("exp.csv")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    with open(csv, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n" 
            +"aa,bb,2,test\n"
        )  # fmt: skip
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv4(cli, db_setup, tmpdir):
    """setting project as filename"""
    csv = tmpdir.join("test.csv")
    csv.write("device_id,device_manufacturer,qty\n")
    csv.write("aa,bb,1\n", mode="a")
    csv.write("aa,bb,1\n", mode="a")
    csv.write("aa,bb,1\n", mode="a")
    csv.write("aa,bb,1\n", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    csv.write("device_id,device_manufacturer,qty,project\n", mode="w")
    csv.write("aa,bb,4,test\n", mode="a")
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv5(cli, db_setup, tmpdir):
    """import again and remove old"""
    csv = tmpdir.join("test.csv")
    with open(csv, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n" 
            + "aa,bb,1,test\n"
        )  # fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv", "-o"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv51(cli, db_setup, tmpdir):
    """import again , add qty on conflict"""
    csv = tmpdir.join("test.csv")
    with open(csv, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty,project\n" 
            + "aa,bb,1,test\n"
        )  # fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e", "%"])
    bom_import(args)
    exp = pd.read_csv(exp)
    assert int(exp.loc[0, "qty"]) == 2


def test_scan_files1(cli, db_setup, tmpdir):
    """expected behaviour"""
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--reimport"])
    assert set(scan_files(args)) == set([csv1.strpath, csv2.strpath])


def test_scan_files2(cli, db_setup, capsys):  # pylint: disable=unused-argument
    """empty DB"""
    args = cli.parse_args(["bom", "--reimport"])
    with pytest.raises(SystemExit):
        scan_files(args)
    out, _ = capsys.readouterr()
    assert "projects to reimport" in out.lower()


def test_scan_files3(cli, db_setup, tmpdir, capsys):
    """file deleted"""
    test1 = tmpdir.join("test1.csv")
    with open(test1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty\n"
            + "aa,bb,1"
        )  # fmt:skip
    test2 = tmpdir.join("test2.csv")
    with open(test2, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty\n"
            + "aa,bb,1"
        )  # fmt:skip

    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    test1.remove()
    args = cli.parse_args(["bom", "--reimport"])
    scan_files(args)
    out, _ = capsys.readouterr()
    assert "test1.csv" in out.lower()


def test_scan_files4(cli, db_setup, tmpdir):
    """no files"""
    csv1 = tmpdir.join("test1.csv")
    with open(csv1, "w", encoding="UTF8") as f:
        f.write(
                "device_id,device_manufacturer,qty\n"
                + "aa,bb,1"
        )  # fmt: skip
    csv2 = tmpdir.join("test2.csv")
    with open(csv2, "w", encoding="UTF8") as f:
        f.write(
                "device_id,device_manufacturer,qty\n"
                + "aa,bb,1"
        )  # fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    csv1.remove()
    csv2.remove()
    args = cli.parse_args(["bom", "--reimport"])
    with pytest.raises(SystemExit):
        scan_files(args)


def test_export_show_all_projects(cli, db_setup, tmpdir, capsys):
    """show all projects possible to export"""
    csv1 = tmpdir.join("test1.csv")
    with open(csv1, "w", encoding="UTF8") as f:
        f.write(
                "device_id,device_manufacturer,qty\n"
                + "aa,bb,1"
        )  # fmt: skip
    csv2 = tmpdir.join("test2.csv")
    with open(csv2, "w", encoding="UTF8") as f:
        f.write(
                "device_id,device_manufacturer,qty\n"
                + "aa,bb,1"
        )  # fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--export", "?"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "test1" in out.lower()
    assert "test2" in out.lower()


def test_bom_import_overwrite(db_setup, cli, tmpdir):
    """
    Tests the --overwrite functionality of bom_import.
    """
    # 1. Setup initial stock file
    stock_file_1 = tmpdir.join("stock1.csv")
    with open(stock_file_1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty\n"
            + "dev1,MAN_A,10\n"
            + "dev2,MAN_B,20\n"
        )
    args = cli.parse_args(["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv"])
    bom_import(args)

    # 2. Verify initial import
    stock1 = getDF(tab="BOM", follow=True)
    assert stock1.loc[stock1[DEV_ID] == "dev1", BOM_QTY].iloc[0] == 10
    assert stock1.loc[stock1[DEV_ID] == "dev2", BOM_QTY].iloc[0] == 20

    # 3. Setup second stock file with different quantities
    stock_file_1 = tmpdir.join("stock1.csv")
    with open(stock_file_1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty\n"
            + "dev1,MAN_A,5\n"
            + "dev2,MAN_B,5\n"
            ) # fmt: skip

    # 4. Import without --overwrite (should add quantities)
    args = cli.parse_args(["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv"])
    bom_import(args)
    stock2 = getDF(tab="BOM", follow=True)
    assert stock2.loc[stock2[DEV_ID] == "dev1", BOM_QTY].iloc[0] == 15
    assert stock2.loc[stock2[DEV_ID] == "dev2", BOM_QTY].iloc[0] == 25

    # 5. Import with --overwrite (should overwrite quantities)
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv", "--overwrite"]
    )
    bom_import(args)
    stock3 = getDF(tab="BOM", follow=True)
    assert stock3.loc[stock3[DEV_ID] == "dev1", BOM_QTY].iloc[0] == 5
    assert stock3.loc[stock3[DEV_ID] == "dev2", BOM_QTY].iloc[0] == 5


def test_stock_import_overwrite_no_existing_data(db_setup, cli, tmpdir):
    """
    Tests the --overwrite functionality when no stock.
    """
    # 1. Setup initial stock file
    stock_file_1 = tmpdir.join("stock1.csv")
    with open(stock_file_1, "w", encoding="UTF8") as f:
        f.write(
            "device_id,device_manufacturer,qty\n"
            + "dev1,MAN_A,10\n"
            + "dev2,MAN_B,20\n"
        )
    args = cli.parse_args(
        ["stock", "-d", tmpdir.strpath, "-f", "stock1", "-F", "csv", "--overwrite"]
    )
    bom_import(args)

    # 2. Verify initial import
    stock1 = getDF(tab="BOM", follow=True)
    assert stock1.loc[stock1[DEV_ID] == "dev1", BOM_QTY].iloc[0] == 10
    assert stock1.loc[stock1[DEV_ID] == "dev2", BOM_QTY].iloc[0] == 20
