"""configuration file"""

import os
import sys

import pandas as pd
from tomlkit import TOMLDocument
from tomlkit.exceptions import ParseError
from tomlkit.toml_file import TOMLFile

import conf.sql_colnames as colnames  # pylint: disable=unused-import

# keep sql colnames in separate file to avoid circular import
from conf.sql_colnames import *  # pylint: disable=unused-wildcard-import,wildcard-import

# list of keywords to be ignored during reading columns from tab
SQL_KEYWORDS = ["FOREIGN", "UNIQUE", "ON_CONFLICT", "HASH_COLS", "COL_DESCRIPTION"]
TAKE_LONGER_COLS = [DEV_MAN, DEV_DESC, DEV_PACK]
HIDDEN_COLS = [
    BOM_DIR,
    BOM_FILE,
    BOM_FORMAT,
    "id",
    DEV_HASH,
    BOM_HASH,
    SHOP_DATE,
]  # columns automatically filled, no need to import
NO_EXPORT_COLS = [
    DEV_HASH,
    BOM_HASH,
    SHOP_HASH,
    "id",
]  # columns not exported
IMPORT_FORMAT_SPECIAL_KEYS = ["cols", "dtype", "func", "file_ext", "shop"]

# Determine the absolute path of the installed module root.
# This is done by getting the path of this config file and going up one level.
_DEFAULT_CONFIG_PATH = os.path.dirname(os.path.abspath(__file__))
MODULE_PATH = os.path.dirname(_DEFAULT_CONFIG_PATH)

# START OF TOML CONFIGURATION
# TOML file name with configuration
TOML_FILE = "inventory.toml"


def find_toml() -> str:
    """
    Search for a local 'inventory.toml'. Falls back to the
    default config TOML if no local one is found.
    Search along the path from current location (including) all the way to root
    """
    current_dir = os.getcwd()
    # default configuration
    conf_path = os.path.join(MODULE_PATH, "conf")
    # search local configuration
    while True:
        conf_loc_path = os.path.join(current_dir, ".config")
        if os.path.exists(conf_loc_path):
            conf_loc_files = os.listdir(conf_loc_path)
            if TOML_FILE in conf_loc_files:
                conf_path = conf_loc_path
                break
        parent = os.path.dirname(current_dir)
        if parent == current_dir:  # reached top
            break
        current_dir = parent
    return conf_path


def read_TOML(loc: str) -> TOMLDocument:  # pylint: disable=invalid-name
    """
    read configuration from TOML file
    loc is file location, do not perform any search
    """
    try:
        toml_file = TOMLFile(loc)
        toml_conf = toml_file.read()
    except FileNotFoundError:
        print(f"File not found. Expected: {loc}")
        sys.exit(1)
    except ParseError as e:
        print("Wrong file content")
        print(e)
        print(f"file location: {loc}")
        sys.exit(1)
    return toml_conf


def replace_path(path: str, file: str) -> str:
    """replace dir"""
    filename = os.path.basename(file)
    return os.path.join(path, filename)


def write_TOML(path: str):  # pylint: disable=invalid-name
    """
    modify directories in TOML file,
    from whatever to path
    use default TOML in app.conf as reference
    also make sure to write default vals if missing
    """
    base_conf = read_TOML(os.path.join(MODULE_PATH, "conf", TOML_FILE))
    # modify dir in DB_FILE, LOG_FILE, MAN_ALT
    base_conf["DB_FILE"] = replace_path(
        path,
        base_conf.get("DB_FILE", "inventory.sqlite"),
    )
    base_conf["LOG_FILE"] = replace_path(
        path,
        base_conf.get("LOG_FILE", "log.txt"),
    )
    base_conf["MAN_ALT"] = replace_path(
        path,
        base_conf.get("MAN_ALT", "manufacturer_alternatives.jsonc"),
    )
    base_conf["BOM_EXPORT_COL"] = base_conf.get(
        "BOM_EXPORT_COL",
        [
            "BOM_PROJECT",
            "BOM_QTY",
            "DEV_ID",
            "DEV_MAN",
            "DEV_PACK",
            "DEV_DESC",
            "DEV_CAT1",
            "DEV_CAT2",
        ],
    )
    base_conf["STOCK_EXPORT_COL"] = base_conf.get(
        "STOCK_EXPORT_COL",
        [
            "STOCK_QTY",
            "DEV_ID",
            "DEV_MAN",
            "DEV_PACK",
            "DEV_DESC",
            "DEV_CAT1",
            "DEV_CAT2",
        ],
    )
    base_conf["COL_WIDTH"] = base_conf.get(
        "COL_WIDTH",
        {
            "STOCK_QTY": 4,
            "DEV_ID": 17,
            "DEV_MAN": 15,
            "DEV_PACK": 8,
            "DEV_DESC": 30,
            "DEV_CAT1": 25,
            "DEV_CAT2": 25,
            "BOM_PROJECT": 12,
            "BOM_QTY": 3,
            "SHOP_ID": 17,
        },
    )
    toml_dest = TOMLFile(os.path.join(path, TOML_FILE))
    toml_dest.write(base_conf)


# make sure the default is correct
write_TOML(os.path.join(MODULE_PATH, "conf"))

# This can be local TOML or fallback to global
CONFIG_PATH = find_toml()

toml_loc = read_TOML(os.path.join(CONFIG_PATH, TOML_FILE))
toml_def = read_TOML(os.path.join(MODULE_PATH, "conf", TOML_FILE))

# database file location and name
DB_FILE = str(toml_loc.get("DB_FILE", toml_def["DB_FILE"]))

# log file location and name
# set LOG_FILE = '' to turn off
LOG_FILE = str(toml_loc.get("LOG_FILE", toml_def["LOG_FILE"]))

# file with manufacturer alternative names
MAN_ALT = str(toml_loc.get("MAN_ALT", toml_def["MAN_ALT"]))

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
SCAN_DIR = toml_loc["SCAN_DIR"]
SCAN_DIR = str(toml_loc.get("SCAN_DIR", toml_def["SCAN_DIR"]))
# scan_dir = ""

# to include subdirectories in SCAN_DIR?
INCLUDE_SUB_DIR = str(toml_loc.get("INCLUDE_SUB_DIR", toml_def["INCLUDE_SUB_DIR"]))

# display currency
# only for display, no any conversion is made
DISP_CURR = str(toml_loc.get("DISP_CURR", toml_def["DISP_CURR"]))

# directory for temporary files
TEMP_DIR = str(toml_loc.get("TEMP_DIR", toml_def["TEMP_DIR"]))

# columns to export
BOM_EXPORT_COL = toml_loc.get("BOM_EXPORT_COL", toml_def["BOM_EXPORT_COL"])
BOM_EXPORT_COL = [globals().get(c, None) for c in BOM_EXPORT_COL]
STOCK_EXPORT_COL = toml_loc.get("STOCK_EXPORT_COL", toml_def["STOCK_EXPORT_COL"])
STOCK_EXPORT_COL = [globals().get(c, None) for c in STOCK_EXPORT_COL]
COL_WIDTH = toml_loc.get("COL_WIDTH", toml_def["COL_WIDTH"])
COL_WIDTH = {globals().get(k, None): v for k, v in COL_WIDTH.items()}
# END OF TOML CONFIGURATION

# handle behavior of user interctive components
# - 'none'      normal interaction
# - 'debugpy'   debuging with debugpy (detectd in inv.main())
#               fire separate console and wait for it to end
# - 'pytest'    completely ignore interactive elements
DEBUG = "none"

# location to SQL scheme
SQL_SCHEME = os.path.join(MODULE_PATH, "conf", "sql_scheme.jsonc")


# FILE IMPORT FORMATTERS
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


def easyEDA(row: pd.Series) -> pd.Series:  # pylint: disable=invalid-name
    """merge column 'value' with 'description'"""
    if "value" in row.index and "device_description" in row.index:
        if not bool(pd.isna(row["value"])):
            row["device_description"] = (
                str(row["value"]) + " : " + str(row["device_description"])
            )
    return row


def csvLCSC(row: pd.Series) -> pd.Series:  # pylint: disable=invalid-name
    """merge column 'value' with 'description'"""
    if "order_qty" in row.index:
        row["order_qty"] = row["order_qty"].split("\\")[0]  # pyright: ignore
    return row


# pased to pandas read_excel() function as args and kwargs
# csv format is passed to read_csv()
# special keys, not passed to pandas import:
# 'cols' - columns renaming with pandas.rename()
# 'dtype' - columns type, passed to pandas.asype()
#           use pandas object, Int64 (capital I !!)
#           or float64 (also for int) to allow proper
#           NaN handling (Int64 do not have NaN)
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
            "qty": "Int64",
            "order_qty": "Int64",
            "price": "float64",
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
        "dtype": {
            "qty": "Int64",
            "package": str,
        },  # lower case only, after cols rename!
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
            "qty": "Int64",
            "price": "float64",
        },
        "func": mouser,
        "shop": "mouser",
    },
    "csv": {
        "file_ext": ["csv"],
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "dtype": {"qty": "Int64", "price": "float64"},
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
        "dtype": {"qty": "Int64", "price": "float64"},
        "func": csvLCSC,
        "shop": "LCSC",
    },
}
