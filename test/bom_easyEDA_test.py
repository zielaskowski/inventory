"""BOM.py testing for easyEDA file format"""

import pandas as pd
import pytest

from app.bom import bom_import
from app.sql import sql_check
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


def test_bom_import_easyEDA1(cli, monkeypatch, tmpdir):
    """import and export without errors"""
    monkeypatch.setattr("conf.config.DB_FILE", tmpdir.strpath + "db.sql")
    monkeypatch.setattr("conf.config.SCAN_DIR", "")
    monkeypatch.setattr("conf.config.DEBUG", "pytest")
    sql_check()

    test = tmpdir.join("test.csv")
    with open(test, mode="w", encoding="UTF8") as f:
        f.write(
            "Quantity,Value,Manufacturer Part,Manufacturer,Name,Primary Category,"
            + "Secondary Category,Description,Supplier Footprint\n"
            + "3,1uF,CC0603KRX5R8BB105,YAGEO(国巨),1uF,Capacitors,"
            + "Multilayer Ceramic Capacitors MLCC - SMD/SMT,Capacitance: Tolerance:±10% "
            + "Tolerance:±10% Voltage Rated: Temperature Coefficient:,0603"
        )
    raw = pd.read_csv(test)
    raw.to_excel(tmpdir.join("text.xls"), engine="openpyxl")

    args = cli.parse_args(["bom", "-d", tmpdir.strpath])
    bom_import(args)

    exp = tmpdir.join("exp.csv")
    exp.write("")
    args = cli.parse_args(["bom", "-d", exp.dirname, "-f", exp.basename, "-e", "%"])
    bom_import(args)

    inp = tmpdir.join("input.csv")
    with open(inp, mode="w", encoding="UTF8") as f:
        f.write(
            "qty,device_id,device_manufacturer,dev_category1,dev_category2,"
            + "device_description,package\n"
            + "3,CC0603KRX5R8BB105,YAGEO,Capacitors,Multilayer Ceramic Capacitors "
            + "MLCC - SMD/SMT,1uF : Capacitance: Tolerance:10% Tolerance:10% "
            + "Voltage Rated: Temperature Coefficient:,0603"
        )
    inp = pd.read_csv(inp)
    exp = pd.read_csv(exp)
    common_cols = exp.columns.intersection(inp.columns)
    exp = exp[common_cols]
    assert exp.equals(inp[common_cols])
