import os
import pandas as pd
from pandas.errors import ParserError
from argparse import Namespace

from app.sql import put
from app.common import check_dir_file, read_json, hash_table
from conf.config import import_format, SQL_scheme
from app.error import messageHandler, write_bomError

msg = messageHandler()


def import_bom(
    args: Namespace,
) -> None:
    xls_files = check_dir_file(args)

    # import all xlsx files
    for file in xls_files:
        # go through all files and append to dataframe
        msg.import_file(file)
        try:
            new_stock = pd.read_excel(
                file,
                **{
                    k: v
                    for k, v in import_format[args.format].items()
                    if k
                    not in [
                        "cols",
                        "dtype",
                    ]
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

        # rename (and tidy) columns according to format of imported file
        new_stock = __columns_align__(
            new_stock.copy(),
            file=file,
            supplier=args.format,
        )
        try:
            write_bom(
                dat=new_stock.copy(),
                qty=args.qty,
                file=file,
                format=import_format[args.format]["header"],
            )
        except write_bomError as e:
            print(e)
            continue

        try:
            write_shop(
                dat=new_stock.copy(),
                file=file,
                format=import_format[args.format]["header"],
            )
        except write_bomError as e:
            print(e)
            continue
            
def write_shop(dat: pd.DataFrame, file: str, row_shift: int) -> None:
    pass

def write_bom(dat: pd.DataFrame, qty: int, file: str, row_shift: int) -> None:
    # check columns: mandatary, nice to have
    # and hash, also from foreign tables
    (
        BOM_must_cols,
        BOM_nice_cols,
        BOM_hash_cols,
    ) = __tab_cols__("BOM")
    
    # check if all required columns are in new_stock
    missing_cols = [c for c in BOM_must_cols if c not in dat.columns]
    if any(missing_cols):
        raise write_bomError(f"Missing mandatory columns: {missing_cols}")

    # multiply by qty or ask for value
    if qty == -1:
        qty = input(f"Provide 'Qty' multiplyer for {file}: ")
    dat["qty"] = dat["qty"] * qty

    # remove rows with NA in must_cols
    dat = __NA_rows__(
        dat,
        BOM_must_cols,
        BOM_nice_cols,
        row_shift=row_shift + 2,  # one for header, and one to start from zero
    )

    # hash rows
    for hashed_col in BOM_hash_cols:
        hash_cols = BOM_hash_cols[hashed_col]
        dat[hashed_col] = dat.apply(
            lambda x: hash_table(
                "BOM",
                x,
                hash_cols,
            ),
            axis=1,
        )

    # put into SQL
    # need to start from DEVICE becouse BOM refer to it
    put(
        dat=dat,
        tab="DEVICE",
        on_conflict=[{"action": "IGNORE", "unique_col": ["hash"]}],
    )

    put(
        dat=dat,
        tab="BOM",
        on_conflict=[
            {
                "action": "UPDATE SET",
                "unique_col": ["device_hash"],
                "add_col": ["qty"],  # UPDATE will add new value to existing
            },
        ],
    )


def __NA_rows__(
    df: pd.DataFrame,
    must_cols: list[str],
    nice_cols: list[str],
    row_shift: int,
) -> pd.DataFrame:
    # check rows with any NA

    # inform user and remove from new_stock, remove only when must rows missing
    na_rows = df[
        df.loc[
            :,
            must_cols,
        ]
        .isna()
        .any(axis=1)
    ]
    na_rows_int = na_rows.index.values + row_shift
    print(f"Missing necessery data in rows: {na_rows_int}. Skiping these rows.")
    df = df[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df[
        df.loc[
            :,
            nicer_cols,
        ]
        .isna()
        .any(axis=1)
    ]
    print("These rows have NAs in NON essential columns:")
    print(na_rows)

    return df


def __tab_cols__(
    tab: str,
) -> list[list[str], list[str], dict[str : list[str]],]:
    # return list of columns that are required for the given tab
    # and list of columns that are "nice to have"
    # follow FOREIGN key constraints to other tab
    # and check HASH_COLS if exists in other tab
    sql_scheme = read_json(SQL_scheme)
    if tab not in sql_scheme:
        raise ValueError(f"Table {tab} does not exists in SQL_scheme")

    tab_cols = list(sql_scheme.get(tab))
    must_cols = [c for c in tab_cols if "NOT NULL" in sql_scheme[tab][c]]
    must_cols = [
        c for c in must_cols if "PRIMARY KEY" not in sql_scheme[tab][c]
    ]
    nice_cols = [c for c in tab_cols if "NOT NULL" not in sql_scheme[tab][c]]
    nice_cols = [
        c for c in nice_cols if "PRIMARY KEY" not in sql_scheme[tab][c]
    ]
    hash_dic = {}

    if "HASH_COLS" in tab_cols:
        hashed_col = list(sql_scheme[tab]["HASH_COLS"].keys())[0]
        hash_cols = [v for _, v in sql_scheme[tab]["HASH_COLS"].items()][0]
        must_cols = [c for c in must_cols if c not in hashed_col]
        nice_cols = [c for c in nice_cols if c not in hashed_col]
        hash_dic = {hashed_col: hash_cols}

    if "UNIQUE" in tab_cols:
        for U in sql_scheme[tab]["UNIQUE"]:
            must_cols += [U]
            if U in nice_cols:
                nice_cols.remove(U)

    if "FOREIGN" in tab_cols:
        for F in sql_scheme[tab]["FOREIGN"]:
            col = list(F.keys())[0]
            nice_cols = [c for c in nice_cols if c not in col]
            must_cols = [c for c in must_cols if c not in col]

            # get foreign table and column
            foreign_tab_col = list(F.values())[0]

            # get foreign_tab
            (
                foreign_tab,
                foreign_col,
            ) = foreign_tab_col.split("(")
            foreign_col = foreign_col.replace(")", "")

            # get foreign columns
            (
                foreign_must,
                foreign_nice,
                foreign_hash,
            ) = __tab_cols__(foreign_tab)
            must_cols += foreign_must
            nice_cols += foreign_nice
            hash_dic.update(foreign_hash)
            hash_dic[col] = foreign_hash[foreign_col]

            (
                foreign_must,
                foreign_nice,
                foreign_hash,
            ) = __tab_cols__(foreign_tab)
            must_cols += foreign_must
            nice_cols += foreign_nice
            hash_dic.update(foreign_hash)
            hash_dic[col] = list(foreign_hash.values())[0]

    # remove duplicates
    must_cols = list(set(must_cols))
    nice_cols = list(set(nice_cols))

    # remove COMMANDS and ['id', 'hash] column
    nice_cols = [
        c
        for c in nice_cols
        if c
        not in [
            "FOREIGN",
            "UNIQUE",
            "HASH_COLS",
            "id",
            "hash",
        ]
    ]
    must_cols = [
        c
        for c in must_cols
        if c
        not in [
            "id",
            "hash",
        ]
    ]
    return (
        must_cols,
        nice_cols,
        hash_dic,
    )


def __columns_align__(
    n_stock: pd.DataFrame,
    file: str,
    supplier: str,
) -> pd.DataFrame:
    # rename columns (and lower)
    n_stock.rename(
        columns={c: str(c).lower() for c in n_stock.columns},
        inplace=True,
    )
    n_stock.rename(
        columns=import_format[supplier]["cols"],
        inplace=True,
    )

    # change columns type
    # only for existing cols
    exist_col = [
        c in n_stock.columns for c in import_format[supplier]["dtype"].keys()
    ]
    exist_col_dtypes = {
        k: v
        for k, v in import_format[supplier]["dtype"].items()
        if k in exist_col
    }
    n_stock = n_stock.astype(exist_col_dtypes)

    # add column with path and file name and supplier
    n_stock["dir"] = os.path.dirname(file)
    n_stock["file"] = os.path.basename(file)
    n_stock["supplier"] = supplier

    return n_stock
