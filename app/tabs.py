import os
import re
from datetime import date
import pandas as pd

from app.sql import put
from app.device import align_manufacturer
from app.error import write_bomError
from app.error import messageHandler
from app.common import read_json, hash_table, __tab_cols__
from conf.config import SQL_scheme, import_format

msg = messageHandler()

def write_tab(
    dat: pd.DataFrame, tab: str, qty: int, file: str, row_shift: int
) -> None:
    # prepares and check if data aligned with table.
    # then writes to table

    # check columns: mandatary, nice to have
    # and hash, also from foreign tables
    (
        BOM_must_cols,
        BOM_nice_cols,
        BOM_hash_cols,
    ) = __tab_cols__(tab)

    # check if all required columns are in new_stock
    missing_cols = [c for c in BOM_must_cols if c not in dat.columns]
    if any(missing_cols):
        raise write_bomError(
            f"For table {tab} missing mandatory columns: {missing_cols}"
        )

    # multiply by qty or ask for value
    if "qty" in dat.columns:
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

    # align manufacturer for common device_id
    dat = align_manufacturer(dat)

    # hash rows
    for hashed_col in BOM_hash_cols:
        hash_cols = BOM_hash_cols[hashed_col]
        dat[hashed_col] = dat.apply(
            lambda x: hash_table(
                tab,
                x,
                hash_cols,
            ),
            axis=1,
        )

    # put into SQL
    sql_scheme = read_json(SQL_scheme)
    put(
        dat=dat,
        tab=tab,
        on_conflict=sql_scheme[tab]["ON_CONFLICT"],
    )


def __NA_rows__(
    df: pd.DataFrame,
    must_cols: list[str],
    nice_cols: list[str],
    row_shift: int,
) -> pd.DataFrame:
    # check rows with any NA

    # inform user and remove from new_stock, remove only when must rows missing
    na_rows = df[df.loc[:, must_cols].isna().any(axis=1)]
    na_rows_id: list[int] = [int(c) + row_shift for c in na_rows.index.values]
    msg.na_rows(row_id=na_rows_id)
    df = df[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df[df.loc[:, nicer_cols].isna().any(axis=1)]
    msg.na_rows(rows=na_rows)

    return df




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
    n_stock["shop"] = supplier
    n_stock["date"] = date.today().strftime("%Y-%m-%d")

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    for c in n_stock.columns:
        if n_stock[c].dtype == "object":
            n_stock[c] = n_stock[c].apply(ASCII_txt)
            
    return n_stock


def ASCII_txt(txt:str)->str:
    # remove any chinese signs from string columns
    txt = str(txt).encode('ascii', 'ignore').decode('ascii')
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    return txt