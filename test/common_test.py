"""common function tests"""

import os

import pytest

from app.common import read_json
from app.error import read_jsonError
from conf.config import SQL_SCHEME

JSON_FILE = "./test/sql.json"


def write_json(txt):
    """write txt to a file"""
    with open(JSON_FILE, "w", encoding="UTF8") as f:
        f.writelines(txt)


def test_read_json():
    """test json reading"""
    # read application json sql - no error
    read_json(SQL_SCHEME)

    # mising file
    with pytest.raises(read_jsonError) as err_info:
        read_json("missing_file")
    print(err_info.value)
    assert err_info.match("missing_file")

    # corrupt file - commas
    json_txt = """
    //** test
    "key":"value"
    "key2":"value"
    """
    write_json(json_txt)
    with pytest.raises(read_jsonError) as err_info:
        read_json(JSON_FILE)
    print(err_info.value)
    assert err_info.match(JSON_FILE)

    # corrupt file - comment
    json_txt = """
    // test
    {
    "key":"value",
    "key2":"value"
     }
    """
    write_json(json_txt)
    with pytest.raises(read_jsonError) as err_info:
        read_json(JSON_FILE)
    print(err_info.value)
    assert err_info.match(JSON_FILE)


@pytest.fixture(scope="session", autouse=True)
def cleanup_env():
    """remove temporaru files"""
    yield
    if os.path.isfile(JSON_FILE):
        os.remove(JSON_FILE)
