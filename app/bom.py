import os
import pandas as pd
from pandas.errors import ParserError
from argparse import Namespace

from app.sql import put
from app.common import check_dir_file, read_json
from conf.config import import_format, SQL_scheme
from app.error import columnError


def bom(args: Namespace) -> None:
    xls_files = check_dir_file(args)
    BOM_must_cols, BOM_nice_cols = __tab_cols__("BOM")

    # import all xlsx files
    for f in xls_files:
        # go through all files and append to dataframe
        print()
        print("***********************")
        print(f"importing {os.path.basename(f)} file...")
        try:
            new_stock = pd.read_excel(
                f,
                **{
                    k: v
                    for k, v in import_format[args.format].items()
                    if k not in ["cols", "dtype"]
                },
            )
        except ParserError as e:
            print("Possibly wrong excel format (different shop?)")
            print(e)
            continue
        except ValueError as e:
            print("Possibly 'no matched' row.")
            print(e)
            continue

        try:
            # rename (and tidy) columns according to format of imported file
            new_stock = __columns_align__(new_stock.copy(), f=f, s=args.format)
        except columnError as e:
            print(e)
            continue

        BOM_must_cols, BOM_nice_cols = __tab_cols__("BOM")
        # check if all required columns are in new_stock
        # raise columnError if any is missing
        if not all([c in new_stock.columns for c in BOM_must_cols]):
            print(f"Error: file {f} does not have all necessary columns")
            raise columnError(f"Missing columns are: {BOM_must_cols}")
        
        # multiply by qty or ask for value
        if args.qty == -1:
            args.qty = input(f"Provide 'Qty' mul;tiplyer for {f}: ")
        new_stock["order_qty"] = new_stock["order_qty"] * args.qty

        
        put(dat=new_stock, tab="BOM", on_conflict="add")

        # check columns for shop

        # check columns for dev
       


def __tab_cols__(tab: str) -> list[set[str]]:
    # return list of columns that are required for the given tab
    # and list of columns that are "nice to have"
    # follow FOREIGN key constraints to other tab
    # and check HASH_COLS if exists in other tab
    sql_scheme = read_json(SQL_scheme)
    if tab not in sql_scheme:
        raise ValueError(f"Table {tab} does not exists in SQL_scheme")

    tab_cols = list(sql_scheme.get(tab))
    must_cols = [
        c
        for c in tab_cols
        if any(i in sql_scheme[tab][c] for i in ["NOT NULL", "PRIMARY"])
    ]
    nice_cols = [
        c
        for c in tab_cols
        if all(i not in sql_scheme[tab][c] for i in ["NOT NULL", "PRIMARY"])
    ]

    if "UNIQUE" in tab_cols:
        for U in sql_scheme[tab]["UNIQUE"]:
            must_cols += [U]
            if U in nice_cols:
                nice_cols.remove(U)
    must_cols = list(set(must_cols))

    if "FOREIGN" in tab_cols:
        for F in sql_scheme[tab]["FOREIGN"]:
            col = list(F.keys())[0]
            nice_cols = [c for c in nice_cols if c not in col]
            must_cols += [col]
            
            # get foreign table and column
            foreign_tab_col = list(F.values())[0]
            
            # split foreign_tab_col into table and column
            foreign_tab, foreign_col = foreign_tab_col.split("(")
            foreign_col = foreign_col[:-1]  # remove last ')' from foreign_col
            
            # add HASH_COLS
            if (hs := sql_scheme[foreign_tab].get("HASH_COLS")) is not None:
                must_cols += hs[foreign_col]
                must_cols = [c for c in must_cols if c not in col]

    # remove COMMANDS and ['id', 'hash] column
    nice_cols = [
        c
        for c in nice_cols
        if c not in ["FOREIGN", "UNIQUE", "HASH_COLS", "id", "hash"]
    ]
    must_cols = [c for c in must_cols if c not in ["id", "hash"]]
    return must_cols, nice_cols


def __columns_align__(n_stock: pd.DataFrame, f: str, s: str) -> pd.DataFrame:

    # rename columns (and lower)
    n_stock.rename(columns={c: str(c).lower() for c in n_stock.columns}, inplace=True)
    n_stock.rename(columns=import_format[s]["cols"], inplace=True)

    # change columns type
    n_stock = n_stock.astype(import_format[s]["dtype"])

    # add column with path and file name and supplier
    n_stock["dir"] = os.path.dirname(f)
    n_stock["file"] = os.path.basename(f)
    n_stock["supplier"] = s

    return n_stock
