"""BOM.py testing"""

import pandas as pd
import pytest

from app.bom import bom_import
from app.sql import sql_check
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_bom_no_args(cli, capsys):
    """no files found"""
    args = cli.parse_args(["bom"])
    with pytest.raises(SystemExit) as err_info:
        bom_import(args)
    out, _ = capsys.readouterr()
    assert "no files found" in out.lower()
    assert err_info.value.code == 1


def test_bom_empty_sql(cli, capsys, monkeypatch, tmpdir):
    """empty sql so no files to reimport"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
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
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("device_id,device_manufacturer,qty,project\n")
    csv.write("aa,bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv1(cli, monkeypatch, tmpdir, capsys):
    """import empty csv file
    error: no header in file"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "unexpected error" in out.lower()


def test_bom_import_csv2(cli, monkeypatch, tmpdir, capsys):
    """missing mandatory columns"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("device_manufacturer,qty,project\n")
    csv.write("bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "missing mandatory columns" in out.lower()


def test_bom_import_csv3(cli, monkeypatch, tmpdir, capsys):
    """NAs in mandatory rows"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("device_id,device_manufacturer,qty,project\n")
    csv.write("aa,bb,1,test\n", mode="a")
    csv.write(",bb,1,test\n", mode="a")
    csv.write("aa,,1,test\n", mode="a")
    csv.write("aa,bb,1,test\n", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    out, _ = capsys.readouterr()
    assert "missing necessery" in out.lower()
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    csv.write("device_id,device_manufacturer,qty,project\n", mode="w")
    csv.write("aa,bb,2,test\n", mode="a")
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv4(cli, monkeypatch, tmpdir):
    """setting project as filename"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
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
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
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
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv = tmpdir.join("test.csv")
    csv.write("device_id,device_manufacturer,qty,project\n")
    csv.write("aa,bb,1,test", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv", "-o"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv6(cli, monkeypatch, tmpdir):
    """remove from BOM all"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--remove",'-p','%'])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    assert exp.read() == "\n"


def test_bom_import_csv7(cli, monkeypatch, tmpdir):
    """remove from BOM one project"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--remove", "-p", "test1"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    inp = pd.read_csv(csv2)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])


def test_bom_import_csv9(cli, monkeypatch, tmpdir,capsys):
    """remove from BOM project that do not exists"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()
    csv1 = tmpdir.join("test1.csv")
    csv1.write("device_id,device_manufacturer,qty\n")
    csv1.write("aa,bb,1", mode="a")
    csv2 = tmpdir.join("test2.csv")
    csv2.write("device_id,device_manufacturer,qty\n")
    csv2.write("aa,bb,1", mode="a")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-F", "csv"])
    bom_import(args)
    args = cli.parse_args(["bom", "--remove", "-p", "test"])
    bom_import(args)
    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "exp.csv", "-e"])
    bom_import(args)
    csv = tmpdir.join('csv.csv')
    csv.write("device_id,device_manufacturer,qty\n")
    csv.write("aa,bb,1\n", mode="a")
    csv.write("aa,bb,1", mode="a")
    inp = pd.read_csv(csv)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])
    out,_ = capsys.readouterr()
    assert "no project ['test'] in bom" in out.lower()
