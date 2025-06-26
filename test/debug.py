"""helper for debugging"""

import os

from app.admin import align
from app.bom import bom_import
from app.sql import sql_check
from conf import config as conf
from conf.config import DB_FILE, SCAN_DIR
from inv import cli_parser

#####DEBUG
os.remove(DB_FILE)
conf.DEBUG = "debugpy"
sql_check()
with open(f"./{SCAN_DIR}/bom1.csv", "w", encoding="UTF8") as f:
    f.write(
        "device_id,device_manufacturer,qty,device_description\n"
        + "da,maa,1,desc11\n"
        + "da,mab,1,desc12\n"
        + "db,mbb,9,desc21\n"
        + "db,mbc,9,desc22\n"
        + "db,mcc,1,desc33\n"
        + "dc,mcc,1,desc34\n"
    )# fmt: skip
cli = cli_parser()
args = cli.parse_args(["bom", "-d", ".", "-f", "bom1", "-F", "csv"])
bom_import(args)
dat = align()
