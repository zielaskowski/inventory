"""sql functions test"""

import importlib
import json

import pytest

from app import common, sql
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
    importlib.reload(common)
    sql_create()
    json_txt["BOM"]["FOREIGN"].pop()
    jfile2 = tmpdir.join("json2.txt")
    jfile2.write(json.dumps(json_txt))
    monkeypatch.setattr(conf, "SQL_SCHEME", jfile2)
    importlib.reload(sql)
    importlib.reload(common)
    with pytest.raises(SqlCheckError) as err_info:
        sql_check()
    assert err_info.match("db.sql")
