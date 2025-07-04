"""common function tests"""

import json

import pytest

from app.common import (
    check_dir_file,
    find_files,
    first_diff_index,
    foreign_tabs,
    get_alternatives,
    log,
    read_json_dict,
    read_json_list,
    store_alternatives,
)
from app.error import CheckDirError, ReadJsonError, ScanDirPermissionError
from conf.config import SQL_SCHEME
from inv import cli_parser


def sort_dict(dat: dict[str, list[str]]) -> dict[str, list[str]]:
    """sort dict values (list)"""
    return {k: sorted(v) for k, v in dat.items()}


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_read_json1():
    """test json reading"""
    # read application json sql - no error
    read_json_dict(SQL_SCHEME)


def test_read_json2():
    """mising file"""
    with pytest.raises(ReadJsonError) as err_info:
        read_json_dict("missing_file")
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
    with pytest.raises(ReadJsonError) as err_info:
        read_json_dict(f)
    print(err_info.value)
    assert err_info.match(f.strpath)


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
    with pytest.raises(ReadJsonError) as err_info:
        read_json_dict(f)
    assert err_info.match(f.strpath)


def test_read_json5(tmpdir):
    """corrupt file: list when dict expected"""
    json_txt = """
    {'key1':[1,2,3],
     'key2':[4,5,6]}
    """
    f = tmpdir.join("json.txt")
    f.write(json_txt)
    with pytest.raises(ReadJsonError) as err_info:
        read_json_dict(f)
    assert err_info.match(f.strpath)


def test_read_json6(tmpdir):
    """corrupt file: dict when list expected"""
    json_txt = """
    {'key1':{1:2,2:3,3:4},
     'key2':{4:5,5:6,6:7}
     }
    """
    f = tmpdir.join("json.txt")
    f.write(json_txt)
    with pytest.raises(ReadJsonError) as err_info:
        read_json_list(f)
    assert err_info.match(f.strpath)


def test_log1(monkeypatch, capsys):
    """permission error"""
    monkeypatch.setattr("conf.config.LOG_FILE", "/home/nonexistinguser/log")
    log(["test", "test"])
    out, _ = capsys.readouterr()
    assert "nonexistinguser" in out.lower()


def test_log3(monkeypatch, tmpdir):
    """loging normaly"""
    f = tmpdir.join("log.txt")
    monkeypatch.setattr("conf.config.LOG_FILE", f)
    log(["test", "log"])
    assert "test log" in f.read()


def test_log4(monkeypatch, capsys):
    """no file"""
    monkeypatch.setattr("conf.config.LOG_FILE", "./test/")
    log(["test"])
    out, _ = capsys.readouterr()
    assert "missing filename" in out.lower()


def test_find_files1(monkeypatch):
    """lack of permisions"""
    monkeypatch.setattr("conf.config.import_format", {"csv": {"file_ext": "csv"}})
    with pytest.raises(ScanDirPermissionError) as err_info:
        find_files("/", "csv")
    assert err_info.match("/")


def test_find_files2(monkeypatch, tmpdir):
    """expected bevaviour"""
    file_list = []
    monkeypatch.setattr("conf.config.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("conf.config.SCAN_DIR", "sub")
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
    monkeypatch.setattr("conf.config.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("conf.config.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    for i in range(14):
        d = d.mkdir("sub")
        f = d.join("file" + str(i) + ".csv")
        f.write("test")
        file_list.append(f)
    files = check_dir_file(args)
    assert files == file_list


def test_check_dir_files15(monkeypatch, tmpdir, cli):
    """file filtering"""
    file_list = []
    args = cli.parse_args(
        ["bom", "--file", "sub", "--dir", tmpdir.strpath, "--format", "csv"]
    )
    monkeypatch.setattr("conf.config.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("conf.config.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    s = ""
    for i in range(14):
        d = d.mkdir("sub")
        f = d.join("file" + str(i) + ".csv")
        f.write("test")
        if i == 11:
            s = d.join("sub.csv")
            s.write("test")
        file_list.append(f)

    files = check_dir_file(args)
    assert files == [s]


def test_check_dir_files2(monkeypatch, tmpdir, cli):
    """no files after filtering"""
    file_list = []
    args = cli.parse_args(
        ["bom", "--file", "fila", "--dir", tmpdir.strpath, "--format", "csv"]
    )
    monkeypatch.setattr("conf.config.import_format", {"csv": {"file_ext": "csv"}})
    monkeypatch.setattr("conf.config.SCAN_DIR", "sub")
    d = tmpdir.mkdir("sub")
    for i in range(14):
        d = d.mkdir("sub")
        f = d.join("file" + str(i) + ".csv")
        f.write("test")
        file_list.append(f)
    with pytest.raises(CheckDirError) as err_info:
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
    monkeypatch.setattr("conf.config.SQL_SCHEME", fscheme)
    tabs = foreign_tabs("tab1")
    assert tabs == ["tab2", "tab3"]


def test_store_alternatives1(tmpdir, monkeypatch):
    """default behaviour"""
    man_alts = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "b2", "b3"],
    }
    alternatives = {
        "man_in": ["a4", "cc", "dd", "ee"],
        "man_opt": ["a4", "c", "d", "e"],
    }
    selection = ["aa", "cc", "d", "e"]

    man_json = tmpdir.join("man_alt.json")
    monkeypatch.setattr("conf.config.MAN_ALT", man_json.strpath)
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    with open(man_json.strpath, "w", encoding="UTF8") as f:
        json.dump(man_alts, f)

    store_alternatives(alternatives=alternatives, selection=selection)
    with open(man_json, "r", encoding="UTF8") as f:
        exp = json.load(f)
    imp = {
        "aa": ["a1", "a2", "a3", "a4"],
        "bb": ["b1", "b2", "b3"],
        "d": ["dd"],
        "e": ["ee"],
    }
    exp = sort_dict(exp)
    imp = sort_dict(imp)
    assert exp == imp


def test_store_alternatives2(tmpdir, monkeypatch):
    """check one-to-one condition"""
    man_alts = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "b2", "b3"],
    }
    alternatives = {
        "man_in": ["a4", "cc", "dd", "ee"],
        "man_opt": ["a5 | a6", "c", "d", "e"],
    }
    selection = ["aa", "cc", "d", "e"]

    man_json = tmpdir.join("man_alt.json")
    monkeypatch.setattr("conf.config.MAN_ALT", man_json.strpath)
    with open(man_json.strpath, "w", encoding="UTF8") as f:
        json.dump(man_alts, f)

    store_alternatives(alternatives=alternatives, selection=selection)
    with open(man_json, "r", encoding="UTF8") as f:
        exp = json.load(f)
    imp = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "b2", "b3"],
        "d": ["dd"],
        "e": ["ee"],
    }
    exp = sort_dict(exp)
    imp = sort_dict(imp)
    assert exp == imp


def test_store_alternatives3(tmpdir, monkeypatch):
    """remove alternatives if exists (from all keys)"""
    man_alts = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "a1", "b3"],
        "d": ["dd"],
    }
    alternatives = {
        "man_in": ["a1", "cc", "dd", "ee"],
        "man_opt": ["a5", "c", "d", "e"],
    }
    selection = ["ff", "cc", "d", "e"]

    man_json = tmpdir.join("man_alt.json")
    monkeypatch.setattr("conf.config.MAN_ALT", man_json.strpath)
    with open(man_json.strpath, "w", encoding="UTF8") as f:
        json.dump(man_alts, f)

    store_alternatives(alternatives=alternatives, selection=selection)
    with open(man_json, "r", encoding="UTF8") as f:
        exp = json.load(f)
    imp = {
        "aa": ["a2", "a3"],
        "bb": ["b1", "b3"],
        "d": ["dd"],
        "e": ["ee"],
        "ff": ["a1"],
    }
    exp = sort_dict(exp)
    imp = sort_dict(imp)
    assert exp == imp


def test_get_alternatives1(tmpdir, monkeypatch):
    """default"""
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    man_alts = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "b2", "b3"],
    }
    man = ["a2", "a2", "b3", "c"]
    man_json = tmpdir.join("man_alt.json")
    monkeypatch.setattr("conf.config.MAN_ALT", man_json.strpath)
    with open(man_json.strpath, "w", encoding="UTF8") as f:
        json.dump(man_alts, f)

    man_alt, diff_rows = get_alternatives(man)
    assert man_alt == ["aa", "aa", "bb", "c"]
    assert diff_rows == [True, True, True, False]


def test_first_diff_index():
    """all tests comparing lists"""
    assert first_diff_index(["a", "b", "cc", "d"], ["a", "b", "c", "d"]) == 3
    assert first_diff_index(["a", "b", "c", "d"], ["a", "b", "c", "d", "e"]) == 4
    assert first_diff_index(["a", "b", "c", "d"], ["a", "b", "c", "d"]) == 0
