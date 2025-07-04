"""helper for debugging"""

import os

from app.admin import align
from app.bom import bom_import
from app.sql import sql_check
from conf import config as conf
from inv import cli_parser

#####DEBUG
os.remove(conf.DB_FILE)
conf.DEBUG = "debugpy"
conf.SCAN_DIR = ""
sql_check()
with open("/tmp/bom1.csv", "w", encoding="UTF8") as f:
    f.write(
        "device_id,device_manufacturer,qty,device_description,project,dev_category1,dev_category2,package\n"
        + "da,maa,1,desc11,proj1,cat1,cat2,pack1\n"
        + "da,mab,1,desc12,proj2,cat1,cat2,pack1\n"
        + "db,mbb,9,desc21,proj2,cat1,cat2,pack1\n"
        + "db,mbb,9,desc22,proj1,cat1,cat2,pack1\n"
        + "db,mcc,1,desc33,proj2,cat3,cat4,pack5\n"
        + "dc,mcc,1,desc34,proj2,cat1,cat2,pack1\n"
        )# fmt: skip
cli = cli_parser()
args = cli.parse_args(["bom", "-d", "/tmp", "-f", "bom1", "-F", "csv"])
bom_import(args)
align()
