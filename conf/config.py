"""configuration file"""

import os

import pandas as pd

# configuration globals
SQL_SCHEME = "/home/mi/docs/prog/python/inventory/conf/sql_scheme.jsonc"

# database file location and name
# DB_FILE = "/home/mi/docs/prog/MCU/inventory.sqlite"
DB_FILE = "/home/mi/docs/prog/python/inventory/inventory.sqlite"

# log file location and name
# set LOG_FILE = '' to turn off
LOG_FILE = "/home/mi/docs/prog/python/inventory/conf/log.txt"

# file with manufacturer alternative names
MAN_ALT = "/home/mi/docs/prog/python/inventory/conf/manufacturer_alternatives.jsonc"

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
SCAN_DIR = "BOM"
# scan_dir = ""

# display currency
# only for display, no any conversion is made
DISP_CURR = "$"

# directory for temporary files
TEMP_DIR = "/tmp/"

# handle behavior of user interctive components
# - 'none'      normal interaction
# - 'debugpy'   debuging with debugpy (detectd in inv.main())
#               fire separate console and wait for it to end
# - 'pytest'    completely ignore interactive elements
DEBUG = "none"


def config_file():
    """where am I"""
    return __file__


def module_path():
    """absolute path to the module"""
    abspath = os.path.abspath(__file__)
    return os.path.dirname(abspath)


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
        row["device_description"] = (
            str(row["value"]) + " : " + str(row["device_description"])
        )
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
    },
    "csv": {
        "file_ext": ["csv"],
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "dtype": {"qty": int, "price": float},
        "func": None,
    },
}
