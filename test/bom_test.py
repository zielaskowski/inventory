"""BOM.py testing"""

import pandas as pd
import pytest

from app.admin import admin
from app.import_dat import bom_import, scan_files
from app.sql import sql_check
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


def test_bom_empty_sql(cli, capsys, monkeypatch, tmpdir):
    """empty sql so no files to reimport"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    sql_check()
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


def test_bom_import_csv(cli, monkeypatch, tmpdir):
    """import and export without errors"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_bom_import_csv11(cli, monkeypatch, tmpdir):
    """import and export without errors, strip text during import"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_bom_export(cli, monkeypatch, tmpdir):
    """export with not existing hidden columns"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_bom_import_csv1(cli, monkeypatch, tmpdir, capsys):
    """import empty csv file
    error: no header in file"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "unexpected error" in out.lower()


def test_bom_import_csv2(cli, monkeypatch, tmpdir, capsys):
    """missing mandatory columns"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("device_manufacturer,qty,project\n")
    csv.write("bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "missing columns: ['device_id']" in out.lower()


def test_bom_import_csv3(cli, monkeypatch, tmpdir, capsys):
    """NAs in mandatory rows"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
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


def test_bom_import_csv4(cli, monkeypatch, tmpdir):
    """setting project as filename"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_bom_import_csv5(cli, monkeypatch, tmpdir):
    """import again and remove old"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_bom_import_csv51(cli, monkeypatch, tmpdir):
    """import again , add qty on conflict"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_scan_files1(cli, monkeypatch, tmpdir):
    """expected behaviour"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
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


def test_scan_files2(cli, monkeypatch, tmpdir, capsys):
    """empty DB"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
    args = cli.parse_args(["bom", "--reimport"])
    with pytest.raises(SystemExit):
        scan_files(args)
    out, _ = capsys.readouterr()
    assert "projects to reimport" in out.lower()


def test_scan_files3(cli, monkeypatch, tmpdir, capsys):
    """file deleted"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

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


def test_scan_files4(cli, monkeypatch, tmpdir):
    """no files"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    csv1.remove()
    csv2.remove()
    args = cli.parse_args(["bom", "--reimport"])
    with pytest.raises(SystemExit):
        scan_files(args)


def test_export_show_all_projects(cli, monkeypatch, tmpdir, capsys):
    """show all projects possible to export"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--export", "?"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "test1" in out.lower()
    assert "test2" in out.lower()
