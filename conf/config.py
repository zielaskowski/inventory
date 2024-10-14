# configuration globals
SQL_scheme = "./conf/sql_scheme.jsonc"

# list of keywords to be ignored during reading columns from tab
SQL_keywords = ["FOREIGN", "UNIQUE", "HASH_COLS", "ON_CONFLICT"]

db_file = "./inventory.sqlite"
cols = (
    []
)  # columns, set based on stock_file or first excel to read, using shop['cols] converter
must_cols = ["qty", "order_qty", "manufacturer#"]


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
        "usecols": list(range(0,20)),
        "na_values": "-",
        "cols": {  # lower case only, align with sql_scheme.jsonc
            "quantity": "qty",
            "manufacturer part": "device_id",
            "manufacturer": "device_manufacturer",
            "supplier footprint": "package",
            "description": "device_description",
        },
        "dtype": {  # lower case only, after cols rename!
            "qty": int
        },
    },
}
