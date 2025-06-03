"""common function tests"""

import json

import pytest

from app.common import check_dir_file, find_files, foreign_tabs, log, read_json
from app.error import check_dirError, read_jsonError, scan_dir_permissionError
from conf.config import SQL_SCHEME
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_read_json1():
    """test json reading"""
    # read application json sql - no error
    read_json(SQL_SCHEME)


def test_read_json2():
    """mising file"""
    with pytest.raises(read_jsonError) as err_info:
        read_json("missing_file")
    assert err_info.match("missing_file")


def test_read_json3(tmpdir):
    """corrupt file - commas"""
    json_txt = """
    //** test
    "key":"value"
    "key2":"value"
    """
    f = tmpdir.join("json.txt")
    f.write(json_txt)
    with pytest.raises(read_jsonError) as err_info:
        read_json(f)
    print(err_info.value)
    assert err_info.match(str(f))


def test_read_json4(tmpdir):
    """corrupt file - comment"""
    json_txt = """
    // test
    {
    "key":"value",
    "key2":"value"
     }
    """
    f = tmpdir.join("json.txt")
    f.write(json_txt)
    with pytest.raises(read_jsonError) as err_info:
        read_json(f)
    assert err_info.match(str(f))


def test_log1(monkeypatch, capsys):
    """permission error"""
    monkeypatch.setattr("app.common.LOG_FILE", "/home/nonexistinguser/log")
    log(["test", "test"])
    out, _ = capsys.readouterr()
    assert "nonexistinguser" in out.lower()


def test_log3(monkeypatch, tmpdir):
    """loging normaly"""
    f = tmpdir.join("log.txt")
    monkeypatch.setattr("app.common.LOG_FILE", f)
    log(["test", "log"])
    assert "test log" in f.read()


def test_log4(monkeypatch, capsys):
    """no file"""
    monkeypatch.setattr("app.common.LOG_FILE", "./test/")
    log(["test"])
    out, _ = capsys.readouterr()
    assert "missing filename" in out.lower()


def test_find_files1(monkeypatch):
    """lack of permisions"""
    monkeypatch.setattr("app.common.import_format", {"csv": {"file_ext": "csv"}})
    with pytest.raises(scan_dir_permissionError) as err_info:
        find_files("/", "csv")
    assert err_info.match("/")


def test_find_files2(monkeypatch, tmpdir):
    """expected bevaviour"""
    file_list = []
    monkeypatch.setattr("app.common.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("app.common.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    for _ in range(4):
        d = d.mkdir("sub")
        f = d.join("file.csv")
        f.write("test")
        file_list.append(f)
    files = find_files(tmpdir.strpath, "csv")
    assert files == file_list


def test_check_dir_files1(monkeypatch, tmpdir, cli):
    """expected bevaviour"""
    file_list = []
    args = cli.parse_args(
        ["bom", "--file", "fil", "--dir", tmpdir.strpath, "--format", "csv"]
    )
    monkeypatch.setattr("app.common.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("app.common.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    for i in range(14):
        d = d.mkdir("sub")
        f = d.join("file" + str(i) + ".csv")
        f.write("test")
        file_list.append(f)
    files = check_dir_file(args)
    assert files == file_list


def test_check_dir_files2(monkeypatch, tmpdir, cli):
    """no files after filtering"""
    file_list = []
    args = cli.parse_args(
        ["bom", "--file", "fila", "--dir", tmpdir.strpath, "--format", "csv"]
    )
    monkeypatch.setattr("app.common.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("app.common.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    for i in range(14):
        d = d.mkdir("sub")
        f = d.join("file" + str(i) + ".csv")
        f.write("test")
        file_list.append(f)
    with pytest.raises(check_dirError) as err_info:
        check_dir_file(args)
    assert err_info.match("fila")


def test_foreign_tabs1(monkeypatch, tmpdir):
    """expected behaviour"""
    sql_scheme = {
        "tab1": {
            "col1": "TEXT",
            "col2": "TEXT",
            "col3": "TEXT",
            "FOREIGN": [{"col1": "tab2(col1)"}, {"col2": "tab3(col1)"}],
        },
        "tab2": {"col1": "TEXT", "col2": "TEXT", "col3": "TEXT"},
        "tab3": {"col1": "TEXT", "col2": "TEXT", "col3": "TEXT"},
    }
    fscheme = tmpdir.join("scheme.json")
    fscheme.write(json.dumps(sql_scheme))
    monkeypatch.setattr("app.common.SQL_SCHEME", fscheme)
    tabs = foreign_tabs("tab1")
    assert tabs == ["tab2", "tab3"]
