"""sql functions test"""

import os

import pytest

from app.error import check_dirError, sql_createError
from app.sql import sql_check, sql_create

JSON_FILE = "./test/sql.json"


def write_json(txt):
    """write txt to a file"""
    with open(JSON_FILE, "w", encoding="UTF8") as f:
        f.writelines(txt)


def test_sql_create1(monkeypatch):
    """should be fine"""
    db_file = "./test/db_test.sql"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    sql_create()
    sql_check()


def test_sql_create2(monkeypatch):
    """wrong DB_FILE path"""
    db_file = ".test//db_test.sql"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    with pytest.raises(check_dirError) as err_info:
        sql_create()
    assert err_info.match(".test")


def test_sql_create3(monkeypatch):
    """missing json"""
    db_file = "./test/db_test.sql"
    sql_json = "./test/sql.json"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    monkeypatch.setattr("app.sql.SQL_SCHEME", sql_json)
    with pytest.raises(sql_createError) as err_info:
        sql_create()
    assert err_info.match(sql_json)


def test_sql_create4(monkeypatch):
    """mising TABLE name"""
    json_txt = """
    //** test
    {
    "key":"value",
    "key2":"value"
     }
    """
    write_json(json_txt)
    db_file = "./test/db_test.sql"
    sql_json = "./test/sql.json"
    monkeypatch.setattr("app.sql.DB_FILE", db_file)
    monkeypatch.setattr("app.sql.SQL_SCHEME", sql_json)
    with pytest.raises(sql_createError) as err_info:
        sql_create()
    assert err_info.match(sql_json)


@pytest.fixture(scope="session", autouse=True)
def cleanup_env():
    """remove temporary files"""
    yield
    if os.path.isfile("./test/db_test.sql"):
        os.remove("./test/db_test.sql")
    if os.path.isfile(JSON_FILE):
        os.remove(JSON_FILE)
