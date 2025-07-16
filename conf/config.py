"""configuration file"""

import os

import pandas as pd

from conf.sql_colnames import *


def module_path():
    """
    absolute path to the module:
    one level up from this file
    no '/' at end of path
    """
    abspath = os.path.abspath(__file__)
    file_dir = os.path.dirname(abspath)
    return os.path.dirname(file_dir)


# configuration globals
SQL_SCHEME = module_path() + "/conf/sql_scheme.jsonc"

# database file location and name
DB_FILE = module_path() + "/inventory.sqlite"

# log file location and name
# set LOG_FILE = '' to turn off
LOG_FILE = module_path() + "/conf/log.txt"

# file with manufacturer alternative names
MAN_ALT = module_path() + "/conf/manufacturer_alternatives.jsonc"

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
SCAN_DIR = "BOM"
# scan_dir = ""
# to include subdirectories in SCAN_DIR?
INCLUDE_SUB_DIR = True

# display currency
# only for display, no any conversion is made
DISP_CURR = "$"

# directory for temporary files
TEMP_DIR = "/tmp/"

# columns to export
BOM_EXPORT_COL = [BOM_PROJECT, BOM_QTY, DEV_ID, DEV_MAN, DEV_PACK, DEV_DESC]
STOCK_EXPORT_COL = [STOCK_QTY, DEV_ID, DEV_MAN, DEV_PACK, DEV_DESC]


# handle behavior of user interctive components
# - 'none'      normal interaction
# - 'debugpy'   debuging with debugpy (detectd in inv.main())
#               fire separate console and wait for it to end
# - 'pytest'    completely ignore interactive elements
DEBUG = "none"


def config_file():
    """where am I"""
    return __file__


def mouser(row: pd.Series) -> pd.Series:
    """
    function passed to pandas apply() on DataFrame rows
    (row by row). Format/change value and return new value.
    Used during file import, see import foramtter below
    """
    if "order_qty" in row.index:
        row["order_qty"] = 1
    if "price" in row.index:
        try:
            row["price"] = float(str(row["price"]).replace("$", ""))
        except ValueError:
            # there are some summary rows at end, causing strings in price col
            row["price"] = 0
    return row


def easyEDA(row: pd.Series) -> pd.Series:
    """merge column 'value' with 'description'"""
    if "value" in row.index and "device_description" in row.index:
        if not bool(pd.isna(row["value"])):
            row["device_description"] = (
                str(row["value"]) + " : " + str(row["device_description"])
            )
    return row


def csvLCSC(row: pd.Series) -> pd.Series:
    """merge column 'value' with 'description'"""
    if "order_qty" in row.index:
        row["order_qty"] = row["order_qty"].split("\\")[0]
    return row


# pased to pandas read_excel() function as args and kwargs
# csv format is passed to read_csv()
# special keys, not passed to pandas import:
# 'cols' - columns renaming with pandas.rename()
# 'dtype' - columns type, passed to pandas.dtypes()
# 'func' - function performed on each row with pandas.apply()
# 'file_ext' - file extension for file searching functions
import_format = {
    "LCSC": {
        "file_ext": ["xls", "xlsx"],
        "header": 4,
        "index_col": None,
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "lcsc#.1": "shop_id",
            "quantity": "qty",
            "mrf#.1": "device_id",
            "mfr..1": "device_manufacturer",
            "package.1": "package",
            "description.1": "device_description",
            "order qty.": "order_qty",
            "unit price(usd)": "price",
        },
        "dtype": {  # lower case only, after cols rename!
            "qty": int,
            "order_qty": int,
            "price": float,
        },
        "func": None,
        "shop": "LCSC",
    },
    "easyEDA": {
        "file_ext": ["xls", "xlsx"],
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "quantity": "qty",
            "manufacturer part": "device_id",
            "manufacturer": "device_manufacturer",
            "supplier footprint": "package",
            "description": "device_description",
            "primary category": "dev_category1",
            "secondary category": "dev_category2",
        },
        "dtype": {"qty": int, "package": str},  # lower case only, after cols rename!
        "func": easyEDA,
    },
    "mouser": {
        "file_ext": ["xls", "xlsx"],
        "header": 8,
        "index_col": None,
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "mouser no": "shop_id",
            "order qty.": "order_qty",
            "mfr. no": "device_id",
            "manufacturer": "device_manufacturer",
            "description ": "device_description",
            "price (usd)": "price",
        },
        "dtype": {  # lower case only, after cols rename!
            "qty": int,
            "price": float,
        },
        "func": mouser,
        "shop": "mouser",
    },
    "csv": {
        "file_ext": ["csv"],
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "dtype": {"qty": int, "price": float},
        "func": None,
    },
    "csv_LCSC": {
        "file_ext": ["csv"],
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "cols": {
            "manufacture part number": "device_id",
            "manufacturer": "device_manufacturer",
            "order qty.": "stock_qty",
            "lcsc part number": "shop_id",
            "description": "device_description",
            "unit price($)": "price",
            "min\\mult order qty.": "order_qty",
        },
        "dtype": {"qty": int, "price": float},
        "func": csvLCSC,
        "shop": "LCSC",
    },
}
