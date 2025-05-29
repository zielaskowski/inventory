"""configuration file"""

# configuration globals
SQL_SCHEME = "/home/mi/docs/prog/python/inventory/conf/sql_scheme.jsonc"

# list of keywords to be ignored during reading columns from tab
SQL_KEYWORDS = ["FOREIGN", "UNIQUE", "ON_CONFLICT"]

# database file location and name
DB_FILE = "/home/mi/docs/prog/MCU/inventory.sqlite"

# log file location and name
LOG_FILE = "/home/mi/docs/prog/python/inventory/conf/log.txt"

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
SCAN_DIR = "BOM"
# scan_dir = ""


def mouser(*args, **kwargs) -> float | str:
    """
    function passed to pandas apply() on Dataframe columns
    so Series basically. Format values in columns
    Used during file import, see import foramtter below
    """
    col_name = kwargs.get("col_name")
    col = args[0]
    if col_name == "order_qty":
        return 1
    if col_name == "price":
        try:
            val = float(col.replace("$", ""))
            return val
        except ValueError:
            # there are some summary rows at end, causing strings in price col
            return 0
    return col


# pased to pandas read_excel() function as args and kwargs
import_format = {
    "LCSC": {
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
        "header": 0,
        "index_col": None,
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "quantity": "qty",
            "manufacturer part": "device_id",
            "manufacturer": "device_manufacturer",
            "supplier footprint": "package",
            "description": "device_description",
        },
        "dtype": {"qty": int},  # lower case only, after cols rename!
        "func": None,
    },
    "mouser": {
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
}
