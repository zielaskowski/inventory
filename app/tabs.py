"""
helper function for importing
clean and tidy tables, but also manage files scaning
"""

import hashlib
import os
import re
import subprocess
import sys
from argparse import Namespace
from datetime import date

import pandas as pd

from app import sql
from app.common import (
    BOM_COMMITED,
    BOM_DIR,
    BOM_FILE,
    BOM_FORMAT,
    BOM_HASH,
    BOM_PROJECT,
    DEBUG,
    DEV_DESC,
    DEV_HASH,
    DEV_ID,
    DEV_MAN,
    SHOP_DATE,
    SHOP_SHOP,
    check_dir_file,
    foreign_tabs,
    get_alternatives,
    match_from_list,
    read_json_dict,
    store_alternatives,
    tab_cols,
    unpack_foreign,
    vimdiff_config,
)
from app.error import (
    ambigous_matchError,
    check_dirError,
    no_matchError,
    prepare_tabError,
    scan_dir_permissionError,
    sql_tabError,
)
from app.message import messageHandler
from conf.config import SQL_SCHEME, TEMP_DIR, import_format

msg = messageHandler()


def import_tab(dat: pd.DataFrame, tab: str, args: Namespace, file: str) -> None:
    """prepare tab as per config and sql_scheme and import"""
    # rename (and tidy) columns according to format of imported file
    # apply configuration from config.py
    dat = columns_align(
        dat.copy(),
        file=file,
        args=args,
    )

    # if 'price' in columns and device_id
    if "price" in dat.columns and tab == "BOM":
        print('Add shop cart with "shop_cart_import" command.')
        print("skiping this file.")
        return

    # existing device for summary info reasons
    ex_devs = sql.getL(tab="DEVICE", get=[DEV_HASH])

    try:
        # align table with sql definition
        # remove NAs in mandatory columns
        dat = prepare_tab(
            dat=dat.copy(),
            tab=tab,
            file=file,
            row_shift=import_format[args.format]["header"],
        )
    except prepare_tabError as e:
        msg.msg(str(e))
        return

    # check if data already in sql, only for BOM tab
    if tab == "BOM" and not check_existing_data(dat, args, file):
        return  # user do not want to overwrite nor add to existing data

    # check for different manufacturer on the same dev_id
    # must be before HASH but after tab preparations and alignment!!
    dat = align_column_duplications(dat, merge_on=DEV_ID, duplication=DEV_MAN)

    # hash columns - must be last so all columns aligned and present
    dat = hash_tab(tab=tab, dat=dat)

    # check for different descriptions on the same dev_hash
    # must be after hash and all other formating
    dat = align_column_duplications(dat, merge_on=DEV_HASH, duplication=DEV_DESC)

    # write data to SQL
    # need to start from DEVICE because other tables refer to it
    sql_scheme = read_json_dict(SQL_SCHEME)
    for t in foreign_tabs(tab) + [tab]:
        sql.put(
            dat=dat,
            tab=t,
            on_conflict=sql_scheme[t].get("ON_CONFLICT", {}),
        )

    # SUMMARY
    msg.BOM_import_summary(dat, len(dat[dat[BOM_HASH].isin(ex_devs)]))


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
    """
    must_cols, nice_cols = tab_cols(tab)

    # check if all required columns are in new_stock
    missing_cols = [c for c in must_cols if c not in dat.columns]
    # if 'project' column is missing, take file name col, inform user
    if BOM_PROJECT in missing_cols:
        dat[BOM_PROJECT] = dat[BOM_FILE].apply(lambda cell: cell.split(".")[0])
        missing_cols = [c for c in missing_cols if c != BOM_PROJECT]
        msg.project_as_filename()
    # must afetr project column creation, otherway possibly missing
    if any(missing_cols):
        raise prepare_tabError(tab, file, missing_cols)

    # remove rows with NA in must_cols
    dat = NA_rows(dat, must_cols, nice_cols, row_shift)

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    # MUST be after NA_rows, becouse ASCI_txt affect NaNs
    for c in dat.columns:
        if dat[c].dtype == "object":
            dat[c] = dat[c].apply(ASCII_txt)

    return dat


def hash_tab(tab: str, dat: pd.DataFrame) -> pd.DataFrame:
    """
    hash columns as per foreign key in SQLscheme
    also unpack foreign key to make sure all columns present
    """

    def apply_hash(row: pd.Series, cols: list[str]) -> str:
        combined = "".join(str(row[c]) for c in cols)
        return hashlib.sha256(combined.encode()).hexdigest()

    def hash_t(t):
        hash_cols = sql_scheme[t].get("HASH_COLS", [])
        if hash_cols:
            dat["hash"] = dat.apply(lambda row: apply_hash(row, hash_cols), axis=1)

    sql_scheme = read_json_dict(SQL_SCHEME)
    tabs = [tab] + foreign_tabs(tab)

    for t in tabs:
        hash_t(t)

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
    # lower columns
    n_stock.rename(
        columns={c: str(c).lower() for c in n_stock.columns},
        inplace=True,
    )
    # drop columns if any col in values so to avoid duplication
    if cols := import_format[args.format].get("cols"):
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
    if f := import_format[args.format].get("func"):
        n_stock = n_stock.apply(f, axis=1, result_type="broadcast")  # type: ignore

    # change columns type
    # only for existing cols
    if dtype := import_format[args.format].get("dtype"):
        exist_col = [c in n_stock.columns for c in dtype.keys()]
        exist_col_dtypes = {k: v for k, v in dtype.items() if k in exist_col}
        n_stock = n_stock.astype(exist_col_dtypes)

    # Strip whitespace from all string elements in the DataFrame
    n_stock = n_stock.map(lambda x: x.strip() if isinstance(x, str) else x)

    # add column with path and file name and supplier
    n_stock[BOM_COMMITED] = False
    n_stock[BOM_DIR] = os.path.abspath(args.dir)
    n_stock[BOM_FILE] = os.path.basename(file)
    n_stock[BOM_FORMAT] = args.format
    n_stock[SHOP_DATE] = date.today().strftime("%Y-%m-%d")
    n_stock[SHOP_SHOP] = args.format

    return n_stock


def ASCII_txt(txt: str) -> str:
    # remove any chinese signs from string columns
    txt = str(txt).encode("ascii", "ignore").decode("ascii")
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    return txt


def align_column_duplications(
    dat: pd.DataFrame,
    merge_on: str,
    duplication: str,
) -> pd.DataFrame:
    """
    Example for manufacturer duplication, analogously work for other (DEV_HASH,DEV_DESC)
    For device_id duplication, collect manufacturer name if different
    create dictionary with list of manufacturers as values for device_id as key:
    {DEV_ID:[MAN1,MAN2,...]}
    1.  checking only incoming data against stock. Do not check duplication
        inside incoming data
    2.  simplest case is one-to-one or many-to-one: only one device in stock
    3.  tricky is when are many devices in stock: present manufacturers separated with
        '|' as list. Special macro in vim to pick option
    return DataFrame modified by user
    """
    if not all(c in dat.columns for c in [merge_on, duplication]):
        return dat
    ex_dat = sql.getDF(
        tab="DEVICE",
        get=[merge_on, duplication],
        search=dat[merge_on].to_list(),
        where=[merge_on],
    )
    if ex_dat.empty:
        # no duplications in stock
        return dat

    # group existing data on dev, join manufacturers with ' | ' if more then one dev
    ex_grp = (
        ex_dat.groupby(merge_on)[duplication]
        .agg(lambda x: " | ".join(x.astype(str)))
        .reset_index()
    )

    # panda RULES: must be merged on 'left' to keep indexes, and clean up NAs later
    # with merge on 'inner' indexed are fuck up
    dat_dup = pd.merge(
        left=dat,
        right=ex_grp,
        on=merge_on,
        how="left",
        suffixes=("", "_stock"),
    )

    # column name
    duplication_stock = duplication + "_stock"

    # for GREAT panda NaN is any value so must be droped before
    dat_dup = dat_dup.loc[~dat_dup[duplication_stock].isna(), :]

    # drop rows with matched manufacturer
    dat_dup = dat_dup[dat_dup[duplication] != dat_dup[duplication_stock]]

    if dat_dup.empty:
        # importing manufacturers are aligned with stock
        return dat

    # if we have stored alternatives, use it
    if DEV_MAN == duplication:
        dat_dup.loc[:, duplication] = get_alternatives(dat_dup[duplication].to_list())

    chosen = select_column(
        alternatives={
            duplication: dat_dup.loc[:, duplication].to_list(),
            duplication_stock: dat_dup.loc[:, duplication_stock].to_list(),
        },
        column=duplication,
    )
    dat_dup[duplication] = chosen
    # put new data into orginal tab, based on preserved indexes
    dat.loc[dat_dup.index, duplication] = dat_dup[duplication]

    return dat


def select_column(alternatives: dict[str, list[str]], column: str) -> list[str]:
    """present alternatives in vimdiff"""
    panel_name = []  # first item is left panel, second is right panel
    for panel in alternatives:
        panel_name += [TEMP_DIR + panel + ".txt"]
        with open(TEMP_DIR + panel + ".txt", mode="w", encoding="UTF8") as f:
            for manufacturer in alternatives[panel]:
                f.write(manufacturer + "\n")

    vimdiff_config(panel_name, column)
    if not DEBUG:
        subprocess.run("vim -u " + TEMP_DIR + ".vimrc", shell=True, check=False)
    with open(panel_name[0], mode="r", encoding="UTF8") as f:
        chosen = f.readlines()

    chosen = [c.strip() for c in chosen]
    if any(len(chosen) != len(v) for v in alternatives.values()):
        # if user mess-up and added/removed rows
        return alternatives[list(alternatives.keys())[0]]
    # write 1to1 matches selected by user, so next time save some time
    if DEV_MAN == column:
        store_alternatives(alternatives=alternatives, selection=chosen)
    return chosen


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
    can abreviate names
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


def scan_files(args) -> list[str]:
    """scan for files to be imported for BOM"""
    if "reimport" in args:
        reimport = args.reimport
    else:
        reimport = False
    if not reimport:
        try:
            files = check_dir_file(args)
        except check_dirError as err:
            msg.msg(str(err))
            sys.exit(1)
        except scan_dir_permissionError as err:
            msg.msg(str(err))
            sys.exit(1)
        if not files:
            msg.import_missing_file()
            sys.exit(1)

    else:
        locations = sql.getDF(
            tab="BOM",
            get=[BOM_DIR, BOM_FILE, BOM_PROJECT, BOM_FORMAT],
            search=["False"],
            where=[BOM_COMMITED],
        )
        if locations.empty:
            msg.reimport_missing_file()
            sys.exit(1)
        files = []
        for _, r in locations.iterrows():
            args.dir = r[BOM_DIR]
            args.filter = r[BOM_FILE]
            args.format = r[BOM_FORMAT]
            f = check_dir_file(args)
            if not f:
                msg.reimport_missing_file(
                    file=str(r[BOM_FILE]),
                    project=str(r[BOM_PROJECT]),
                )
                continue
            files += f
        if not files:
            msg.reimport_missing_file()
            sys.exit(1)
        args.overwrite = True
    return list(set(files))


def bom_info(tab: str, silent: bool = False) -> list[str]:
    """diplay info about columns in BOM table"""
    try:
        must_col, nice_col = tab_cols(tab)
    except sql_tabError as err:
        msg.msg(str(err))
        sys.exit(1)
    if not silent:
        msg.BOM_info(must_col, nice_col, col_description())
    return must_col + nice_col


def bom_template(tab: str, args: Namespace) -> None:
    """save csv tempalete to a file"""
    cols = pd.Series(bom_info(tab=tab, silent=True))
    csv = pd.DataFrame(columns=cols)
    csv.to_csv(args.csv_template, index=False)
    msg.msg(f"template written to {args.csv_template}")


def col_description() -> dict:
    """extract columns descriptions from sql_scheme"""
    sql_scheme = read_json_dict(SQL_SCHEME)
    col_desc = {}
    for _, cont in sql_scheme.items():
        if "COL_DESCRIPTION" in cont:
            for k, v in cont["COL_DESCRIPTION"].items():
                if k not in col_desc:
                    col_desc[k] = v
    return col_desc
