import hashlib
import os
import re
from argparse import Namespace
from datetime import date

import pandas as pd

from app import sql
from app.common import (
    BOM_COMMITED,
    BOM_DIR,
    BOM_FILE,
    BOM_FORMAT,
    BOM_PROJECT,
    DEV_ID,
    TAKE_LONGER_COLS,
    foreign_tabs,
    match_from_list,
    read_json,
    tab_cols,
    unpack_foreign,
)
from app.error import (
    ambigous_matchError,
    messageHandler,
    no_matchError,
    prepare_tabError,
)
from conf.config import SQL_SCHEME, import_format

msg = messageHandler()


def prepare_tab(
    dat: pd.DataFrame,
    tab: str,
    file: str,
    row_shift: int,
) -> pd.DataFrame:
    """
    prepares and check if data aligned with table.
    iterate through tabs and check if mandatory columns present
    return only tables with all mandatory columns
    and sanitazed data

    check columns: mandatary, nice to have
    and hash, also from foreign tables
    """
    must_cols, nice_cols = tab_cols(tab)

    # check if all required columns are in new_stock
    missing_cols = [c for c in must_cols if c not in dat.columns]
    # if 'project' column is missing, take file name col, inform user
    if BOM_PROJECT in missing_cols:
        dat[BOM_PROJECT] = dat[BOM_FILE].apply(lambda cell: cell.split(".")[0])
        missing_cols = [c for c in missing_cols if c != BOM_PROJECT]
        msg.project_as_filename()
    if any(missing_cols):
        msg.column_miss(missing_cols, file, tab)
        raise prepare_tabError(tab, missing_cols)
    # remove rows with NA in must_cols
    dat = NA_rows(dat, must_cols, nice_cols, row_shift)

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    # MUST be after NA_rows, becouse ASCI_txt affect NaNs
    for c in dat.columns:
        if dat[c].dtype == "object":
            dat[c] = dat[c].apply(ASCII_txt)

    sql_scheme = read_json(SQL_SCHEME)
    tabs = ["BOM"] + foreign_tabs("BOM")

    # hash
    def apply_hash(row: pd.Series, cols: list[str]) -> str:
        combined = "".join(str(row[c]) for c in cols)
        return hashlib.sha256(combined.encode()).hexdigest()

    def hash_tab(t):
        hash_cols = sql_scheme[t].get("HASH_COLS", [])
        if hash_cols:
            dat["hash"] = dat.apply(lambda row: apply_hash(row, hash_cols), axis=1)

    for t in tabs:
        hash_tab(t)

    # add foreign col in case it's not present yet
    # for example if we have FOREIGN:[{'dev_hash':'dev(hash)'}]
    # col hash exists, but now we need to copy hash to dev_hash
    for t in tabs:
        for f in sql_scheme[t].get("FOREIGN", []):
            to_col, _, from_col = unpack_foreign(f)
            dat[to_col] = dat[from_col]

    return dat


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


def columns_align(n_stock: pd.DataFrame, file: str, args: Namespace) -> pd.DataFrame:
    supplier = args.format
    # lower columns
    n_stock.rename(
        columns={c: str(c).lower() for c in n_stock.columns},
        inplace=True,
    )
    # drop columns if any col in values so to avoid duplication
    if cols := import_format[supplier].get("cols"):
        n_stock.drop(
            [v for _, v in cols.items() if v in n_stock.columns],
            axis="columns",
            inplace=True,
        )
        # then rename
        n_stock.rename(
            columns=cols,
            inplace=True,
        )

    # apply formatter functions
    if f := import_format[supplier].get("func"):
        n_stock = n_stock.apply(f, axis=1, result_type="broadcast")  # type: ignore

    # change columns type
    # only for existing cols
    if dtype := import_format[supplier].get("dtype"):
        exist_col = [c in n_stock.columns for c in dtype.keys()]
        exist_col_dtypes = {k: v for k, v in dtype.items() if k in exist_col}
        n_stock = n_stock.astype(exist_col_dtypes)

    # add column with path and file name and supplier
    n_stock[BOM_COMMITED] = False
    n_stock[BOM_DIR] = args.dir
    n_stock[BOM_FILE] = os.path.basename(file)
    n_stock[BOM_FORMAT] = args.format
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
    for _, group in tab:
        # take the longest description
        for col in TAKE_LONGER_COLS:
            if col in group.columns:
                row_desc = group.loc[
                    group[col].apply(lambda x: len(x) if pd.notnull(x) else 0).idxmax(),
                    col,
                ]
                group[col] = row_desc
        ret_tab = pd.concat([ret_tab, group.iloc[1].to_frame().T], ignore_index=True)
    return ret_tab


def check_existing_data(dat: pd.DataFrame, args: Namespace, file: str) -> bool:
    """
    check if data already present in sql
    if -overwrite, remove existing data
    other way ask for confirmation
    return True if we can continue
    """
    file_name = os.path.basename(file)
    old_files = sql.getL(tab="BOM", get=[BOM_FILE])
    old_project = sql.getL(tab="BOM", get=[BOM_PROJECT])
    if args.overwrite:
        # remove all old data
        sql.rm(
            tab="BOM",
            value=dat[BOM_PROJECT].to_list(),
            column=[BOM_PROJECT],
        )
        return True
    # warn about adding qty
    if file_name in old_files or dat[BOM_PROJECT].unique() in old_project:
        if not msg.file_already_imported(file_name):
            return False
    return True


def prepare_project(projects: list[str], commited: bool) -> list[str]:
    """
    prepare list of projects based on provided args:
    - '%' all projects
    - '?' just list projects
    if 'commited==False', limit search to not commited projects only
    """
    if commited:
        commit_search = ["%"]
    else:
        commit_search = ["False"]
    available = sql.getL(
        tab="BOM",
        get=[BOM_PROJECT],
        search=commit_search,
        where=[BOM_COMMITED],
    )
    all_projects = sql.getL(
        tab="BOM",
        get=[BOM_PROJECT],
    )
    if projects == ["?"]:
        msg.BOM_prepare_projects(
            project=available,
            available=available,
            all_projects=all_projects,
        )
        return []
    if projects == ["%"]:
        projects = available
    match_projects = []
    for project in projects:
        try:
            match_projects += [match_from_list(project, available)]
        except ambigous_matchError as err:
            print(err)
        except no_matchError as err:
            print(err)

    if not any(p in available for p in match_projects):
        msg.BOM_prepare_projects(
            project=match_projects,
            available=available,
            all_projects=all_projects,
        )
    return [p for p in match_projects if p in available]
