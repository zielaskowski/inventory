"""sql functions test"""

import importlib
import json
import os

import pytest

from app import admin, common, sql, sql_core
from app.error import (
    CheckDirError,
    SqlCheckError,
    SqlCreateError,
)
from app.sql import sql_check, sql_create
from conf import config as conf


def test_sql_create1(db_setup):
    """should be fine"""
    sql_create()
    sql_check()


def test_sql_create2(monkeypatch):
    """wrong DB_FILE path"""
    db_file = ".test//db_test.sql"
    monkeypatch.setattr(conf, "DB_FILE", db_file)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    with pytest.raises(CheckDirError) as err_info:
        sql_create()
    assert err_info.match(".test")


def test_sql_create3(monkeypatch):
    """missing json"""
    db_file = "./test/db_test.sql"
    sql_json = "./test/sql.json"
    monkeypatch.setattr(conf, "DB_FILE", db_file)
    monkeypatch.setattr(conf, "SQL_SCHEME", sql_json)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    with pytest.raises(SqlCreateError) as err_info:
        sql_create()
    assert err_info.match(sql_json)


def test_sql_create4(monkeypatch, tmpdir):
    """mising TABLE name - expecting dict"""
    json_txt = """
    //** test
    {
    "key":"value",
    "key2":"value"
     }
    """
    jfile = tmpdir.join("json.txt")
    jfile.write(json_txt)
    monkeypatch.setattr(conf, "DB_FILE", tmpdir + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    with pytest.raises(SqlCreateError) as err_info:
        sql_create()
    assert err_info.match(jfile.strpath)


def test_sql_create5(monkeypatch, tmpdir):
    """misspell sql keyword"""
    json_txt = """
    //** test
    "TAB":{
    "key":"value",
    "key2":"value"
     }
    """
    jfile = tmpdir.join("json.txt")
    jfile.write(json_txt)
    monkeypatch.setattr(conf, "DB_FILE", tmpdir + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    with pytest.raises(SqlCreateError) as err_info:
        sql_create()
    assert err_info.match(jfile.basename)


def test_sql_check1(monkeypatch, tmpdir):
    """change in FOREIGN"""
    json_txt = {
        "DEVICE": {
            "device_hash": "TEXT PRIMARY KEY",
            "device_id": "TEXT NOT NULL",
        },
        "PROJECT": {"project": "TEXT PRIMARY KEY", "project_name": "TEXT"},
        "BOM": {
            "id": "INTEGER PRIMARY KEY",
            "device_hash": "TEXT NOT NULL",
            "project": "TEXT NOT NULL",
            "FOREIGN": [
                {"device_hash": "DEVICE(device_hash)"},
                {"project": "PROJECT(project_name)"},
            ],
        },
    }
    jfile = tmpdir.join("json.txt")
    jfile.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    importlib.reload(sql_core)
    importlib.reload(common)
    sql_create()
    json_txt["BOM"]["FOREIGN"].pop()
    jfile2 = tmpdir.join("json2.txt")
    jfile2.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile2)
    importlib.reload(sql)
    importlib.reload(sql_core)
    importlib.reload(common)
    with pytest.raises(SqlCheckError) as err_info:
        sql_check()
    assert err_info.match("db.sql")


def test_sql_check2(monkeypatch, tmpdir, capsys):
    """sql scheme error: column value not string"""
    json_txt = {
        "DEVICE": {
            "device_hash": ["TEXT PRIMARY KEY"],
            "device_id": "TEXT NOT NULL",
        },
        "PROJECT": {"project": "TEXT PRIMARY KEY", "project_name": "TEXT"},
        "BOM": {
            "id": "INTEGER PRIMARY KEY",
            "device_hash": "TEXT NOT NULL",
            "project": "TEXT NOT NULL",
            "FOREIGN": [
                {"device_hash": "DEVICE(device_hash)"},
                {"project": "PROJECT(project_name)"},
            ],
        },
    }
    jfile = tmpdir.join("json.txt")
    jfile.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    importlib.reload(common)
    with pytest.raises(SqlCreateError):
        sql_check()
    out, _ = capsys.readouterr()
    assert "expected: 'String'" in out


def test_sql_check3(monkeypatch, tmpdir, capsys):
    """sql scheme error: foreign value not list"""
    json_txt = {
        "DEVICE": {
            "device_hash": "TEXT PRIMARY KEY",
            "device_id": "TEXT NOT NULL",
        },
        "PROJECT": {"project": "TEXT PRIMARY KEY", "project_name": "TEXT"},
        "BOM": {
            "id": "INTEGER PRIMARY KEY",
            "device_hash": "TEXT NOT NULL",
            "project": "TEXT NOT NULL",
            "FOREIGN": "str",
        },
    }
    jfile = tmpdir.join("json.txt")
    jfile.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    importlib.reload(common)
    with pytest.raises(SqlCreateError):
        sql_check()
    out, _ = capsys.readouterr()
    assert "expected: 'List'" in out


def test_sql_check4(monkeypatch, tmpdir, capsys):
    """sql scheme error: foraign value not list of dict"""
    json_txt = {
        "DEVICE": {
            "device_hash": "TEXT PRIMARY KEY",
            "device_id": "TEXT NOT NULL",
        },
        "PROJECT": {"project": "TEXT PRIMARY KEY", "project_name": "TEXT"},
        "BOM": {
            "id": "INTEGER PRIMARY KEY",
            "device_hash": "TEXT NOT NULL",
            "project": "TEXT NOT NULL",
            "FOREIGN": [
                ["device_hash", "DEVICE(device_hash)"],
                ["project", "PROJECT(project_name)"],
            ],
        },
    }
    jfile = tmpdir.join("json.txt")
    jfile.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    importlib.reload(common)
    with pytest.raises(SqlCreateError):
        sql_check()
    out, _ = capsys.readouterr()
    assert "expected: 'List of Dict'" in out


def test_sql_check5(monkeypatch, tmpdir, capsys):
    """sql scheme error: hash_cols value not list"""
    json_txt = {
        "DEVICE": {
            "device_hash": "TEXT PRIMARY KEY",
            "device_id": "TEXT NOT NULL",
        },
        "PROJECT": {"project": "TEXT PRIMARY KEY", "project_name": "TEXT"},
        "BOM": {
            "id": "INTEGER PRIMARY KEY",
            "device_hash": "TEXT NOT NULL",
            "project": "TEXT NOT NULL",
            "FOREIGN": [
                {"device_hash": "DEVICE(device_hash)"},
                {"project": "PROJECT(project_name)"},
            ],
            "HASH_COLS": "STR",
        },
    }
    jfile = tmpdir.join("json.txt")
    jfile.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile)
    monkeypatch.setattr(conf, "LOG_FILE", "")
    importlib.reload(sql)
    importlib.reload(common)
    with pytest.raises(SqlCreateError):
        sql_check()
    out, _ = capsys.readouterr()
    assert "expected: 'List'" in out


def test_sql_upgrade(db_setup, cli, tmpdir, monkeypatch):
    """upgrading db as expected"""
    os.remove(conf.DB_FILE)
    sql_create(one_tab="DEVICE")
    man_alts = {
        "aa": ["a1", "a2", "a3"],
        "bb": ["b1", "b2", "b3"],
    }
    man_json = tmpdir.join("man_alt.json")
    monkeypatch.setattr(conf, "MAN_ALT", man_json.strpath)
    importlib.reload(admin)
    importlib.reload(sql)
    with open(man_json.strpath, "w", encoding="UTF8") as f:
        json.dump(man_alts, f)
    args = cli.parse_args(["admin", "--sql_upgrade"])
    admin.admin(args)
    # check if tables exist
    assert "DEVICE" in sql.sql_tables()
    assert "MANUFACTURER" in sql.sql_tables()
    assert "ALTERNATIVE_MANUFACTURER" in sql.sql_tables()
    # check if imported manufacturers
    alt_man = sql.get_man_alternatives()
    assert alt_man == man_alts


def test_sql_no_audite(db_setup):
    """missing audite table"""
    sql.__sql_execute__(["DROP TABLE 'audite_changefeed'"])
    with pytest.raises(SqlCheckError) as err_info:
        sql.sql_check()
    assert err_info.match("Consider upgrading DB file")


def test_sql_no_manufacturer(db_setup):
    """missing manufacturer table"""
    sql.__sql_execute__(["DROP TABLE 'MANUFACTURER'"])
    with pytest.raises(SqlCheckError) as err_info:
        sql.sql_check()
    assert err_info.match("Consider upgrading DB file")
