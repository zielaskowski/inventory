"""pytest units"""

import pytest

from app.admin import align
from app.bom import bom_import
from app.sql import sql_check
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_align_man1(monkeypatch, tmpdir, cli):
    """basic test"""
    monkeypatch.setattr("app.sql.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("app.common.SCAN_DIR", "")
    sql_check()

    bom1 = tmpdir.join("bom1.csv")
    with open(bom1, "w", encoding="UTF8") as f:
        f.write(
        "device_id,device_manufacturer,qty,device_description\n"
        + "da,maa,1,desc11\n"
        + "da,mab,1,desc12\n"
        + "db,mbb,9,desc21\n"
        + "db,mbc,9,desc22\n"
        + "db,mcc,1,desc33\n"
        + "dc,mcc,1,desc34\n"
        )# fmt: skip
    args = cli.parse_args(["bom", "-d", tmpdir.strpath, "-f", "bom1", "-F", "csv"])
    bom_import(args)

    align_dat = align()
