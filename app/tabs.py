import os
import re
from datetime import date

import pandas as pd

from app.common import tab_cols
from app.error import messageHandler, prepare_tabError
from app.sql import getDF
from conf.config import import_format

msg = messageHandler()

DEV_ID = "device_id"
DEV_MAN = "device_manufacturer"
DEV_DESC = "device_description"
DEV_PACK = "package"
cols = [DEV_MAN, DEV_DESC, DEV_PACK]


def prepare_tab(
    dat: pd.DataFrame, tabs: list[str], file: str, row_shift: int
) -> tuple[list[str], pd.DataFrame]:
    """
    prepares and check if data aligned with table.
    iterate through tabs and check if mandatory columns present
    return only tables with all mandatory columns
    and sanitazed data

    check columns: mandatary, nice to have
    and hash, also from foreign tables
    """
    must_cols, nice_cols, missing_cols = [], [], []
    wrong_tab = []
    for tab in tabs:
        mc, nc = tab_cols(tab)

        # check if all required columns are in new_stock
        missing_cols = [c for c in mc if c not in dat.columns]
        if any(missing_cols):
            if tab != "DEVICE":
                # error on DEVICE will be catched by prepare_tabError()
                msg.column_miss(missing_cols, file, tab)
            wrong_tab.append(tab)
            tabs.remove(tab)
            continue
        must_cols += mc
        nice_cols += nc

    must_cols = list(set(must_cols))
    nice_cols = list(set(nice_cols))

    if tabs == [] or "DEVICE" not in tabs:
        raise prepare_tabError(wrong_tab, missing_cols)

    # remove rows with NA in must_cols
    dat = NA_rows(dat, must_cols, nice_cols, row_shift)

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    # MUST be after NA_rows, becouse ASCI_txt affect NaNs
    for c in dat.columns:
        if dat[c].dtype == "object":
            dat[c] = dat[c].apply(ASCII_txt)

    # align manufacturer for common device_id
    all_dat = getDF(tab="DEVICE")
    if not all_dat.empty:
        dat = align_manufacturer(dat, all_dat)

    return tabs, dat


def NA_rows(
    df: pd.DataFrame,
    must_cols: list[str],
    nice_cols: list[str],
    row_shift: int,
) -> pd.DataFrame:
    """
    check rows with any NA

    inform user and remove from data,
    remove only when NA in must rows
    """
    row_shift = +2  # one for header, and one to start from zero
    na_rows = df.loc[df.loc[:, must_cols].isna().any(axis=1)]
    na_rows_id: list[int] = [int(c) + row_shift for c in na_rows.index.values]
    df = df.loc[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df.loc[df.loc[:, nicer_cols].isna().any(axis=1)]
    msg.na_rows(rows=na_rows, row_id=na_rows_id)
    return df


def columns_align(
    n_stock: pd.DataFrame,
    file: str,
    supplier: str,
) -> pd.DataFrame:
    # lower columns
    n_stock.rename(
        columns={c: str(c).lower() for c in n_stock.columns},
        inplace=True,
    )
    # drop columns if any col in values so to avoid duplication
    n_stock.drop(
        [v for _, v in import_format[supplier]["cols"].items() if v in n_stock.columns],
        axis="columns",
        inplace=True,
    )
    # then rename
    n_stock.rename(
        columns=import_format[supplier]["cols"],
        inplace=True,
    )

    # apply formatter functions
    if f := import_format[supplier]["func"]:
        for c in n_stock.columns:
            n_stock[c] = n_stock[c].apply(f, axis=1, col_name=c)

    # change columns type
    # only for existing cols
    exist_col = [c in n_stock.columns for c in import_format[supplier]["dtype"].keys()]
    exist_col_dtypes = {
        k: v for k, v in import_format[supplier]["dtype"].items() if k in exist_col
    }
    n_stock = n_stock.astype(exist_col_dtypes)

    # add column with path and file name and supplier
    file = os.path.abspath(file)
    n_stock["dir"] = os.path.dirname(file)
    n_stock["file"] = os.path.basename(file)
    n_stock["shop"] = supplier
    n_stock["date"] = date.today().strftime("%Y-%m-%d")

    return n_stock


def ASCII_txt(txt: str) -> str:
    # remove any chinese signs from string columns
    txt = str(txt).encode("ascii", "ignore").decode("ascii")
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    return txt


def align_manufacturer(new_tab: pd.DataFrame, dat: pd.DataFrame) -> pd.DataFrame:
    # For device_id duplication, choose longer manufacturer name
    # and longer description
    # group by device_id

    # split new_tab into two parts:
    #  new_tab with device_id not in dat = mod_tab
    mod_tab = new_tab[~new_tab[DEV_ID].isin(dat[DEV_ID])]
    # device_id already in db
    dup_tab = new_tab[~new_tab[DEV_ID].isin(mod_tab[DEV_ID])]

    if dup_tab.empty:
        return mod_tab

    # merging
    # group check_tab by device_id,
    dup_tab = long_description(dup_tab, dat, by=[DEV_ID])
    return pd.concat([mod_tab, dup_tab], ignore_index=True)


def long_description(
    tab1st: pd.DataFrame, tab2nd: pd.DataFrame, by=list[str]
) -> pd.DataFrame:
    # take the longest description for each grouped by
    # and spread all over the group
    # return first row from group

    # from tab2nd take only rows with 'by' columns in tab1st
    tab2nd = tab2nd.merge(tab1st, on=by, how="inner", suffixes=("", "_y"))
    tab = pd.concat([tab1st, tab2nd], ignore_index=True)
    tab = tab.groupby(DEV_ID).filter(lambda x: len(x) > 1).groupby(DEV_ID)

    ret_tab = pd.DataFrame()
    for name, group in tab:
        # take the longest description
        for col in cols:
            if col in group.columns:
                row_desc = group.loc[
                    group[col].apply(lambda x: len(x) if pd.notnull(x) else 0).idxmax(),
                    col,
                ]
                group[col] = row_desc
        ret_tab = pd.concat([ret_tab, group.iloc[1].to_frame().T], ignore_index=True)
    return ret_tab
