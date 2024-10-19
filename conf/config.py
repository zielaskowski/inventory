# configuration globals
SQL_scheme = "./conf/sql_scheme.jsonc"

# list of keywords to be ignored during reading columns from tab
SQL_keywords = ["FOREIGN", "UNIQUE", "ON_CONFLICT"]

# database file location and name
db_file = "./inventory.sqlite"

# log file location and name
log_file = "./conf/log.txt"

# directory to scan when searching for files
# leave empty if you want to scan anything
# not case sensitive
scan_dir = "BOM"
# scan_dir = ""

# excel format description for imported excell
# options for pandas csv_import + columns renaming to align with sql
import_format = {
    "LCSC": {
        "header": 4,
        "index_col": None,
        "usecols": [7] + list(range(11, 25)),
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
    },
    "easyEDA": {
        "header": 0,
        "index_col": None,
        "usecols": list(range(0, 20)),
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "quantity": "qty",
            "manufacturer part": "device_id",
            "manufacturer": "device_manufacturer",
            "supplier footprint": "package",
            "description": "device_description",
        },
        "dtype": {"qty": int},  # lower case only, after cols rename!
    },
    "mouser": {
        "header": 7,
        'index_col': None,
        "usecols": list(range(0, 9)),
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
           'mouser no' : "shop_id",
            "order qty.": "qty",
            "mfr. no": "device_id",
            "manufacturer": "device_manufacturer",
            "description ": "device_description",
            "price (usd)": "price",
        },
        "dtype": {  # lower case only, after cols rename!
            "qty": int,
            "price": float,
        }
    },
}
