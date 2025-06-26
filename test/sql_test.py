"""sql functions test"""

import json

import pytest

from app.error import (
    CheckDirError,
    ReadJsonError,
    SqlCheckError,
    SqlCreateError,
    SqlSchemeError,
)
from app.sql import sql_check, sql_create


def test_sql_create1(monkeypatch, tmpdir):
    """should be fine"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    sql_create()
    sql_check()


def test_sql_create2(monkeypatch):
    """wrong DB_FILE path"""
    db_file = ".test//db_test.sql"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    with pytest.raises(CheckDirError) as err_info:
        sql_create()
    assert err_info.match(".test")


def test_sql_create3(monkeypatch):
    """missing json"""
    db_file = "./test/db_test.sql"
    sql_json = "./test/sql.json"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    monkeypatch.setattr("app.sql.SQL_SCHEME", sql_json)
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
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir + "db.sql")
    monkeypatch.setattr("app.sql.SQL_SCHEME", jfile)
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
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir + "db.sql")
    monkeypatch.setattr("app.sql.SQL_SCHEME", jfile)
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
        "PROJECT": {"project": "TEXT", "project_name": "TEXT"},
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
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.sql.SQL_SCHEME", jfile)
    monkeypatch.setattr("app.common.SQL_SCHEME", jfile)
    sql_create()
    json_txt["BOM"]["FOREIGN"].pop()
    jfile2 = tmpdir.join("json2.txt")
    jfile2.write(json.dumps(json_txt))
    monkeypatch.setattr("app.sql.SQL_SCHEME", jfile2)
    with pytest.raises(SqlCheckError) as err_info:
        sql_check()
    assert err_info.match("db.sql")
