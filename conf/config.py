# configuration globals
SQL_scheme = "/home/mi/docs/prog/python/inventory/conf/sql_scheme.jsonc"

# list of keywords to be ignored during reading columns from tab
SQL_keywords = ["FOREIGN", "UNIQUE", "ON_CONFLICT"]

# database file location and name
db_file = "/home/mi/docs/prog/MCU/inventory.sqlite"

# log file location and name
log_file = "/home/mi/docs/prog/python/inventory/conf/log.txt"

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
scan_dir = "BOM"
# scan_dir = ""


# excel format description for imported excell
# options for pandas csv_import + columns renaming to align with sql
def mouser(*args, **kwargs) -> list:
    col_name = kwargs.get("col_name")
    col = args[0]
    if col_name == "order_qty":
        col = 1
    elif col_name == "price":
        try:
            col = float(col.replace("$", ""))
        except:
            # there are some summary rows at end, causing strings in price col
            col = 0
    return col


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
