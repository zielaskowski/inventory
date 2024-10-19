import os
import re
from datetime import date
import pandas as pd

from app.sql import getDF
from app.error import prepare_tabError
from app.error import messageHandler
from app.common import hash_table, tab_cols
from conf.config import import_format

msg = messageHandler()

DEV_ID = "device_id"
DEV_MAN = "device_manufacturer"
DEV_DESC = "device_description"
DEV_HASH = 'hash'
cols = [DEV_ID, DEV_MAN, DEV_DESC]

def prepare_tab(
    dat: pd.DataFrame, 
    tabs: list[str], 
    qty: int, 
    file: str, 
    row_shift: int
) -> tuple[list[str], pd.DataFrame]:
    # prepares and check if data aligned with table.
    # iterate through tabs and check if mandatory columns present
    # return only tables with all mandatory columns
    # and sanitazed data

    # check columns: mandatary, nice to have
    # and hash, also from foreign tables
    must_cols, nice_cols, hash_cols = [],[],{}
    for tab in tabs: 
        mc, nc, hc = tab_cols(tab)

        # check if all required columns are in new_stock
        missing_cols = [c for c in mc if c not in dat.columns]
        if any(missing_cols):
            msg.column_miss(missing_cols, file, tab)
            tabs.remove(tab)
            continue
        must_cols += mc
        nice_cols += nc
        hash_cols.update(hc)

    must_cols = list(set(must_cols))
    nice_cols = list(set(nice_cols))

    if tabs==[]:
        raise prepare_tabError(tab, missing_cols)
    
    # remove rows with NA in must_cols
    dat = NA_rows(
        dat,
        must_cols,
        nice_cols,
        row_shift=row_shift + 2,  # one for header, and one to start from zero
    )

    # multiply by qty or ask for value
    if "qty" in dat.columns:
        if qty == -1:
            qty = input(f"Provide 'Qty' multiplyer for {file}: ")
        dat["qty"] = dat["qty"] * qty

    # align manufacturer for common device_id
    all_dat = getDF(tab="DEVICE")
    if not all_dat.empty:
        dat = align_manufacturer(dat, all_dat)

    # hash rows
    for hashed_col in hash_cols:
        hashing_cols = hash_cols[hashed_col]
        dat[hashed_col] = dat.apply(
            lambda x: hash_table(
                x,
                hashing_cols,
            ),
            axis=1,
        )
    return tabs,dat


def NA_rows(
    df: pd.DataFrame,
    must_cols: list[str],
    nice_cols: list[str],
    row_shift: int,
) -> pd.DataFrame:
    # check rows with any NA

    # inform user and remove from data, 
    # remove only when must rows missing
    na_rows = df[df.loc[:, must_cols].isna().any(axis=1)]
    na_rows_id: list[int] = [int(c) + row_shift for c in na_rows.index.values]
    msg.na_rows(row_id=na_rows_id)
    df = df[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df[df.loc[:, nicer_cols].isna().any(axis=1)]
    msg.na_rows(rows=na_rows)

    return df


def columns_align(
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


def ASCII_txt(txt: str) -> str:
    # remove any chinese signs from string columns
    txt = str(txt).encode("ascii", "ignore").decode("ascii")
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    return txt


def align_manufacturer(new_tab: pd.DataFrame, 
                       dat: pd.DataFrame) -> pd.DataFrame:
    # AVOID DUPLICATES OF DEVICE_ID BECOUSE OF DIFFERENT MANUFACTURERS
    # group by device_id
    # report all cases when are more manufacturers for the same device_id
    # - merge to first existing
    # - keep new manufacturer
    # aditionally take longest description whenever possible

    # 1. split new_tab into three parts:
    # 1.1. new_tab with device_id not in dat = mod_tab
    mod_tab = new_tab[~new_tab[DEV_ID].isin(dat[DEV_ID])]
    # 1.2. new_tab with device_id and device_manufacturer in dat + mod_tab = mod_tab
    # take longer description
    mod_tab = pd.concat(
        [
            mod_tab, 
            long_description(new_tab, dat, by=[DEV_ID, DEV_MAN])]
    )
    # 1.3. what left:  new_tab - mod_tab = check_tab
    check_tab = new_tab[~new_tab[DEV_ID].isin(mod_tab[DEV_ID])]
    
    if check_tab.empty:
        return mod_tab

    # show duplicates of device_id
    # and ask what to do
    i = msg.dev_manufacturer_align(
                pd.concat(
                    [
                        check_tab,
                        dat[dat[DEV_ID].isin(check_tab[DEV_ID])]
                    ]).
                sort_values(by=[DEV_ID]).
                set_index(DEV_ID, append=True)
                [cols[1:]]
            )

    if i == "m":
        # merging
        # group check_tab by device_id,
        check_tab = long_description(check_tab, dat, by=[DEV_ID])
        return pd.concat([mod_tab, check_tab], ignore_index=True)
    if i == "k":
        # keep
        # we keep new manufacturer for the same device_id
        return pd.concat([mod_tab, check_tab], ignore_index=True)


def long_description(
    tab1st: pd.DataFrame, tab2nd: pd.DataFrame, by=list[str]
) -> pd.DataFrame:
    # take the longest description for each grouped by
    # and spread all over the group
    # return first row from group

    # from tab2nd take only rows with 'by' columns in tab1st
    tab2nd = tab2nd.merge(tab1st, on=by, how='inner', suffixes=('', '_y'))
    tab = pd.concat([tab1st, tab2nd], ignore_index=True)
    tab = (tab.
           groupby(DEV_ID).
           filter(lambda x: len(x) > 1).
           groupby(DEV_ID))

    ret_tab = pd.DataFrame()
    for name, group in tab:
        # take the longest description
        row = group.loc[
                    group[DEV_DESC].
                    apply(lambda x: len(x) if pd.notnull(x) else 0).
                    idxmax()
                    ]
        group[DEV_DESC] = row[DEV_DESC]
        group[DEV_MAN] = row[DEV_MAN]
        ret_tab = pd.concat(
            [
            ret_tab, 
            group.iloc[1].to_frame().T
            ], 
            ignore_index=True
             )
    return ret_tab
