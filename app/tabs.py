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
from pandas.core.dtypes.cast import NAType

from app import sql
from app.common import (
    check_dir_file,
    first_diff_index,
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
    AmbigousMatchError,
    CheckDirError,
    NoMatchError,
    PrepareTabError,
    ScanDirPermissionError,
    SqlTabError,
    VimdiffSelError,
)
from app.message import MessageHandler
from conf import config as conf
from conf.sql_colnames import *

msg = MessageHandler()


def import_tab(dat: pd.DataFrame, tab: str, args: Namespace, file: str) -> None:
    """prepare tab as per config and sql_scheme and import"""
    # rename (and tidy) columns according to format of imported file
    # apply configuration from config.py
    dat = columns_align(
        dat.copy(),
        file=file,
        args=args,
    )

    # existing device for summary info reasons
    ex_devs = sql.getL(tab="DEVICE", get_col=[DEV_HASH])

    try:
        # align table with sql definition
        # remove NAs in mandatory columns
        dat = prepare_tab(
            dat=dat.copy(),
            tab=tab,
            file=file,
            row_shift=conf.import_format[args.format]["header"],
        )
    except PrepareTabError as e:
        msg.msg(str(e))
        return

    # if we have stored alternatives, use it
    dat.loc[:, DEV_MAN], _ = get_alternatives(dat[DEV_MAN].to_list())

    # hash columns - must be last so all columns aligned and present
    dat = hash_tab(dat=dat)

    # check if data already in sql, only for BOM tab
    if tab == "BOM" and not check_existing_project(dat, args):
        return  # user do not want to overwrite nor add to existing data
    if tab == "STOCK" and not check_existing_data(dat, args):
        return

    # check for different manufacturer on the same dev_id
    # just inform that alignment can be done with admin functions
    align_data(dat=dat, just_inform=True)

    # inform if data useful for other tabs is present
    tabs = tabs_in_data(dat)
    if "SHOP" in tabs and tab != "SHOP":
        msg.msg("Detected data usefull also for SHOP table.")
        msg.msg("Consider importing with 'shop_cart_import' option.")
    # write aligned data to SQL
    for t in ["DEVICE", tab]:
        sql.put(dat=dat, tab=t)

    # SUMMARY
    if tab == "BOM":
        msg.bom_import_summary(dat, len(dat[dat[BOM_HASH].isin(ex_devs)]))


def tabs_in_data(dat: pd.DataFrame) -> list[str]:
    """
    return all tbles which mandatary cols are present in dat
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    # need to start from DEVICE because other tables refer to it
    tabs = sorted(list(sql_scheme.keys()), key=lambda x: (x != "DEVICE", x))
    for t in tabs[:]:
        must_cols, _ = tab_cols(t)
        # check if all required columns are in aligned_dat
        missing_cols = [c for c in must_cols if c not in dat.columns]
        if any(missing_cols):
            tabs.remove(t)
    return tabs


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
    # must after 'project' column creation, otherway possibly missing
    if any(missing_cols):
        raise PrepareTabError(tab, file, missing_cols)

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    for c in dat.columns:
        if dat[c].dtype == "object":
            dat[c] = dat[c].apply(ASCII_txt)

    # remove rows with NA in must_cols
    # must be afterASCII_txt, becouse it is possible to have only chinese in must_col
    dat = NA_rows(dat, must_cols, nice_cols, row_shift)

    # and strip text
    for col in dat.select_dtypes(include=["object"]).columns:
        dat[col] = dat[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    return dat


def hash_tab(dat: pd.DataFrame) -> pd.DataFrame:
    """
    hash columns as per foreign key in SQLscheme
    also unpack foreign key to make sure all columns present
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)

    def apply_hash(row: pd.Series, cols: list[str]) -> str:
        combined = "".join(str(row[c]) for c in cols)
        return hashlib.sha256(combined.encode()).hexdigest()

    def hash_t(t):
        hash_cols = sql_scheme[t].get("HASH_COLS", [])
        if hash_cols:
            dat["hash"] = dat.apply(lambda row: apply_hash(row, hash_cols), axis=1)

    tabs = tabs_in_data(dat)
    for t in tabs[:]:
        tabs += foreign_tabs(t)
    tabs = list(set(tabs))

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
    row_shift: int = 0,
    inform: bool = True,
) -> pd.DataFrame:
    """
    check rows with any NA

    inform user and remove from data,
    remove only when NA in must rows
    """
    row_shift += 2  # one for header, and one to start from zero
    na_rows = df.loc[df.loc[:, must_cols].isna().any(axis=1)]
    na_rows_id: list[int] = [int(c) + row_shift for c in na_rows.index.values]
    df = df.loc[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df.loc[df.loc[:, nicer_cols].isna().any(axis=1)]
    if inform:
        msg.na_rows(rows=na_rows, row_id=na_rows_id)
    return df


def columns_align(n_stock: pd.DataFrame, file: str, args: Namespace) -> pd.DataFrame:
    """basic columns align per config.py"""
    # lower columns
    n_stock.rename(
        columns={c: str(c).lower() for c in n_stock.columns},
        inplace=True,
    )
    # drop columns if any col in values so to avoid duplication
    if cols := conf.import_format[args.format].get("cols"):
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
    if f := conf.import_format[args.format].get("func"):
        n_stock = n_stock.apply(f, axis=1, result_type="broadcast")  # type: ignore

    # change columns type
    # only for existing cols
    if dtype := conf.import_format[args.format].get("dtype"):
        exist_col = [c in n_stock.columns for c in dtype.keys()]
        exist_col_dtypes = {k: v for k, v in dtype.items() if k in exist_col}
        n_stock = n_stock.astype(exist_col_dtypes)

    # Strip whitespace from all string elements in the DataFrame
    n_stock = n_stock.map(lambda x: x.strip() if isinstance(x, str) else x)

    # add column with path and file name and supplier
    n_stock[BOM_DIR] = os.path.abspath(args.dir)
    n_stock[BOM_FILE] = os.path.basename(file)
    n_stock[BOM_FORMAT] = args.format
    n_stock[SHOP_DATE] = date.today().strftime("%Y-%m-%d")
    if SHOP_SHOP not in n_stock.columns:
        n_stock[SHOP_SHOP] = conf.import_format[args.format].get("shop", args.format)

    return n_stock


def ASCII_txt(txt: str | None | NAType) -> str | None | NAType:
    """remove any chinese signs from string columns"""
    if not isinstance(txt, str):
        return txt
    if pd.isna(txt):
        return None
    txt = str(txt).replace("Ω", "ohm")
    txt = txt.encode("ascii", "ignore").decode("ascii")
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    if not txt:  # only chinese letters
        return None
    return txt


def align_data(dat: pd.DataFrame, just_inform: bool = False) -> pd.DataFrame:
    """
    align new data with existing:
        1. align manufacturers
        2. where manufacturer changed, align all other columns
        3. remove devs where manufacturer changed
        4. return aligned data to be added
    if just_inform==True, just collect duplication and display, no action
    return dataframe with changed devices with aditional column 'dev_rm'
    with device hashes before change
    """
    align_dat = align_manufacturers(
        dat.copy(deep=True),
        just_inform=just_inform,
    )
    # user abort
    if align_dat.empty:
        return align_dat
    if just_inform:
        return dat
    # select rows where change
    differ_row = dat[DEV_MAN] != align_dat[DEV_MAN]
    # create new hash after possibly changing manufacturer
    align_dat = hash_tab(dat=align_dat).loc[differ_row, :]
    # keep new device in separate dataframe
    keep_dev_hash = align_dat[DEV_HASH].drop_duplicates().to_list()
    keep_dev = dat[dat[DEV_HASH].isin(keep_dev_hash)]
    if len(keep_dev) != len(keep_dev_hash):
        # new manufacturer name somwhere, device not existing in db
        new_dev_hash = [c for c in keep_dev_hash if c not in dat[DEV_HASH]]
        keep_dev = pd.concat(
            [
                keep_dev,
                align_dat[align_dat[DEV_HASH].isin(new_dev_hash)].drop_duplicates(
                    subset=[DEV_HASH]
                ),
            ],
        )
    # possibly empty if no differ rows
    if not any(differ_row.to_list()):
        return dat
    # mark devs that will be removed (dev_rm and man_rm)
    align_dat = pd.concat(
        [
            dat.loc[differ_row, [DEV_HASH, DEV_MAN]].rename(
                columns={
                    DEV_HASH: "dev_rm",
                    DEV_MAN: "man_rm",
                }
            ),  # fmt: ignore
            align_dat,
        ],
        axis="columns",
    )  # fmt: ignore
    # align all other dev cols
    keep_dev = align_other_cols(rm_dat=align_dat, keep_dat=keep_dev)  # pyright: ignore
    # user abort
    if keep_dev.empty:
        return keep_dev
    # add data from other tabs for devs we are going to remove
    # don't take old hashes, but rehash again
    keep_dev = pd.merge(
        left=keep_dev,
        right=align_dat.loc[:, ["dev_rm", DEV_HASH]],
        on=DEV_HASH,
        how="right",
    )
    keep_dev = sql.getDF_other_tabs(
        dat=keep_dev,
        hash_list=keep_dev["dev_rm"].to_list(),
        merge_on="dev_rm",
    )
    keep_dev = hash_tab(keep_dev)
    return keep_dev


def align_other_cols(rm_dat: pd.DataFrame, keep_dat: pd.DataFrame) -> pd.DataFrame:
    """
    expect DataFrame with one pair replacement per row:
    and columns: dev_rm | man_rm | DEV_HASH | DEV_MAN
    display all other attributes of device to user to choose
    dispalay only not empty and attributes that differ
    return selected attributes attached in row to input dat
    """
    necessery_cols = ["dev_rm", "man_rm", DEV_HASH, DEV_MAN]
    must_cols, _ = tab_cols(tab="DEVICE")
    if not all(c for c in rm_dat.columns if c in must_cols + necessery_cols):
        sys.exit(1)
    keep_dat.set_index(DEV_HASH, inplace=True)
    # collect all useful data from devices we are about to remove
    for idx in rm_dat.index:
        rm_attr = rm_dat.copy(deep=True).loc[idx, :]
        # collect attributes for devices that we want to use
        keep_attr = keep_dat.loc[keep_dat.index == rm_attr[DEV_HASH]].iloc[0, :]
        change_man = keep_attr[DEV_MAN]
        opt_man = rm_attr["man_rm"]
        # if NA in rm_attr or missing col, take from add_attr if present
        for idx, val in keep_attr.items():
            if idx not in rm_attr:
                rm_attr[idx] = val
            if rm_attr[idx] is None:
                rm_attr[idx] = keep_attr[idx]
        # and oposite, then will be easy to remove the same
        # keep nulls in add_attr, so user can change
        for idx, val in rm_attr.items():
            if idx not in keep_attr:
                keep_attr[idx] = val
        # hide attributes which are already aligned
        for idx, val in rm_attr.items():
            if val == keep_attr[idx]:
                rm_attr.pop(idx)
                keep_attr.pop(idx)
        if not keep_attr.empty:
            rm_attr.sort_index(inplace=True)
            keep_attr.sort_index(inplace=True)
            try:
                aligned_dat = vimdiff_selection(
                    ref_col={"column": rm_attr.index.to_list()},
                    change_col={change_man: keep_attr.to_list()},
                    opt_col={opt_man: rm_attr.to_list()},
                    exit_on_change=False,
                )
            except KeyboardInterrupt:
                msg.msg("Interupted by user. Changes discarded.")
                return pd.DataFrame()
            except VimdiffSelError as err:
                # if no change from user, skip
                print(str(err))
                if err.interact:
                    input("Press any key....")
                continue
            keep_attr.iloc[:] = aligned_dat
            keep_dat.update(pd.DataFrame([keep_attr]))
    return keep_dat.reset_index()


def align_manufacturers(dat: pd.DataFrame, just_inform: bool = False) -> pd.DataFrame:
    """
    For device_id duplication, collect manufacturer name if different
    create dictionary with list of manufacturers as values for device_id as key:
    {DEV_ID:[MAN1,MAN2,...]}
    1.  checking only incoming data against stock. Do not check duplication
        inside incoming data
    2.  simplest case is one-to-one or many-to-one: only one device in stock
    3.  tricky is when are many devices in stock: present manufacturers separated with
        '|' as list. Special macro in vim to pick option
    return DataFrame modified by user
    when 'just_inform', just display messages about possible duplications
    and ref to admin funcs
    """
    start_line = 1  # start line for vim (vim count from 1)
    if not all(c in dat.columns for c in [DEV_ID, DEV_MAN]):
        return dat
    ex_dat = sql.getDF(
        tab="DEVICE",
        get_col=[DEV_ID, DEV_MAN],
        search=dat[DEV_ID].to_list(),
        where=[DEV_ID],
    )
    if ex_dat.empty:
        # no duplications in stock
        return dat

    # group existing data on dev, join manufacturers with ' | ' if more then one dev
    ex_grp = (
        ex_dat.groupby(DEV_ID)[DEV_MAN]
        .agg(lambda x: " | ".join(x.astype(str)))
        .reset_index()
    )

    # iterate as long as there is a change from user
    while True:
        # panda RULES: must be merged on 'left' to keep indexes, and clean up NAs later
        # with merge on 'inner' indexed are fuck up
        dat_dup = pd.merge(
            left=dat,
            right=ex_grp,
            on=DEV_ID,
            how="left",
            suffixes=("", "_opts"),
        )

        # column name for grouped manufacturers
        man_grp_col = DEV_MAN + "_opts"

        # for GREAT panda NaN is any value so must be droped before
        dat_dup = dat_dup.loc[~dat_dup[man_grp_col].isna(), :]

        # drop rows with matched manufacturer
        dat_dup = dat_dup[dat_dup[DEV_MAN] != dat_dup[man_grp_col]]

        if dat_dup.empty:
            # importing manufacturers are aligned with stock
            return dat

        # remove dup_col from dup_col_grp
        for r in dat_dup.itertuples():
            dup_col_grp_v = getattr(r, man_grp_col)
            dup_col_v = getattr(r, DEV_MAN)
            opts = str(dup_col_grp_v).split(" | ")
            opts = [o for o in opts if o != dup_col_v]
            dat_dup.loc[r.Index, man_grp_col] = " | ".join(opts)

        if just_inform:
            msg.inform_duplications(dup=dat_dup.loc[:, [DEV_MAN, man_grp_col, DEV_ID]])
            return dat
        # sort data on device_id, much easier to understand in vimdiff
        dat_dup.sort_values(by=DEV_ID, inplace=True)
        try:
            chosen = vimdiff_selection(
                ref_col={"devices": dat_dup.loc[:, DEV_ID].to_list()},
                change_col={DEV_MAN: dat_dup.loc[:, DEV_MAN].to_list()},
                opt_col={man_grp_col: dat_dup.loc[:, man_grp_col].to_list()},
                exit_on_change=True,
                start_line=start_line,
            )
        except KeyboardInterrupt:
            msg.msg("Interupted by user. Changes discarded.")
            return pd.DataFrame()
        except VimdiffSelError as err:
            # user messed up with line numbers
            print(str(err))
            return pd.DataFrame()
        # if no change from user, finish
        if dat_dup[DEV_MAN].to_list() == chosen:
            break
        # find index of change, so can start vim on correct line number
        start_line = first_diff_index(dat_dup[DEV_MAN].to_list(), chosen)
        dat_dup[DEV_MAN] = chosen
        # put new data into orginal tab, based on preserved indexes
        dat.loc[dat_dup.index, DEV_MAN] = dat_dup[DEV_MAN]

    return dat


def vimdiff_selection(
    ref_col: dict[str, list[str]],
    change_col: dict[str, list[str]],
    opt_col: dict[str, list[str]],
    exit_on_change: bool,
    start_line: int = 1,
) -> list[str]:
    """present alternatives in vimdiff"""
    cols = []
    for col in [ref_col, change_col, opt_col]:
        cols.append(next(iter(col)))
        cols.append(next(iter(col.values())))
    ref_k, ref_v = (0, 1)
    change_k, change_v = (2, 3)
    opt_k, opt_v = (4, 5)
    for key in [ref_k, change_k, opt_k]:
        # remove forbiden chars: / \ : * ? " < > |
        cols[key] = re.sub(r'[\/\\:\*\?"<>\|\r\n\t]', "_", cols[key])
        with open(
            conf.TEMP_DIR + cols[key] + ".txt",
            mode="w",
            encoding="UTF8",
        ) as f:
            for ind, item in enumerate(cols[key + 1]):
                f.write(str(ind) + "| " + str(item) + "\n")  # add line number: '1| txt'
                # other way diff may shift rows to align data between columns

    vimdiff_config(
        ref_col=cols[ref_k],
        change_col=cols[change_k],
        opt_col=cols[opt_k],
        alternate_col=cols[change_k],
        exit_on_change=exit_on_change,
        start_line=start_line,
    )

    vim_cmd = "vim -u " + conf.TEMP_DIR + ".vimrc"
    if conf.DEBUG == "none":
        subprocess.run(vim_cmd, shell=True, check=False)
    elif conf.DEBUG == "debugpy":
        with subprocess.Popen("konsole -e " + vim_cmd, shell=True) as p:
            p.wait()

    with open(conf.TEMP_DIR + cols[change_k] + ".txt", mode="r", encoding="UTF8") as f:
        chosen = f.read().splitlines()

    # clean up files
    for key in [ref_k, change_k, opt_k]:
        os.remove(conf.TEMP_DIR + cols[key] + ".txt")

    if chosen == []:  # user interrupt
        raise KeyboardInterrupt
    # remove line numbers
    chosen = [re.sub(r"^\d+\|\s*", "", c) for c in chosen]
    chosen = [c.strip() for c in chosen]
    if any(len(chosen) != len(cols[v]) for v in [ref_v, change_v, opt_v]):
        # if user mess-up or added/removed rows
        max_len = max(len(chosen), len(cols[opt_v]))
        chosen += [None] * (max_len - len(chosen))
        cols[opt_v] += [None] * (max_len - len(cols[opt_v]))
        df = pd.DataFrame({"selected": chosen, "opts": cols[opt_v]})
        raise VimdiffSelError(select=df, interact=DEV_MAN != cols[change_k])
    # write 1to1 matches selected by user, so next time save some time
    if DEV_MAN == cols[change_k]:
        store_alternatives(
            alternatives={
                cols[change_k]: cols[change_v],
                cols[opt_k]: cols[opt_v],
            },
            selection=chosen,
        )
    return chosen


def check_existing_project(dat: pd.DataFrame, args: Namespace) -> bool:
    """
    check if project already present in BOM
    if -overwrite, remove existing data
    other way ask for confirmation
    return True if we can continue
    """
    old_project = sql.getL(tab="BOM", get_col=[BOM_PROJECT])
    project = dat.loc[0, BOM_PROJECT]
    if args.overwrite:
        # remove all old data
        sql.rm(
            tab="BOM",
            value=dat[BOM_PROJECT].to_list(),
            column=[BOM_PROJECT],
        )
        return True
    # warn about adding qty
    if project in old_project:
        if not msg.project_already_imported(project):
            return False
    return True


def check_existing_data(dat: pd.DataFrame, args: Namespace) -> bool:
    """
    check if data already present in STOCK
    if -overwrite, remove existing data
    other way ask for confirmation
    return True if we can continue
    """
    if args.dont_ask:
        return True
    old_data = sql.getL(tab="STOCK", get_col=[STOCK_HASH])
    overlap_data = dat.loc[dat[STOCK_HASH].isin(old_data), :]
    # warn about adding qty
    if not overlap_data.empty:
        if not msg.data_already_imported(overlap_data):
            return False
    return True


def prepare_project(projects: list[str]) -> list[str]:
    """
    prepare list of projects based on provided args:
    - '%' all projects
    - '?' just list projects
    can abreviate names
    """
    all_projects = sql.getL(
        tab="BOM",
        get_col=[BOM_PROJECT],
    )
    if not all_projects:
        msg.bom_prepare_projects([], [])
        return []
    if projects == ["?"]:
        msg.bom_prepare_projects(
            project=[],
            all_projects=all_projects,
        )
        return []
    if projects == ["%"]:
        projects = all_projects
    match_projects = []
    for project in projects:
        try:
            match_projects += [match_from_list(project, all_projects)]
        except AmbigousMatchError as err:
            print(err)
        except NoMatchError as err:
            print(err)

    if match_projects != projects:
        msg.bom_prepare_projects(
            project=match_projects,
            all_projects=[],
        )
    return match_projects


def scan_files(args) -> list[str]:
    """scan for files to be imported for BOM"""
    if "reimport" in args:
        reimport = args.reimport
    else:
        reimport = False
    if not reimport:
        try:
            files = check_dir_file(args)
        except CheckDirError as err:
            msg.msg(str(err))
            sys.exit(1)
        except ScanDirPermissionError as err:
            msg.msg(str(err))
            sys.exit(1)
        if not files:
            msg.import_missing_file()
            sys.exit(1)

    else:
        locations = sql.getDF(
            tab="BOM",
            get_col=[BOM_DIR, BOM_FILE, BOM_PROJECT, BOM_FORMAT],
        )
        if locations.empty:
            msg.reimport_missing_file()
            sys.exit(1)
        files = []
        for _, r in locations.iterrows():
            args.dir = r[BOM_DIR]
            args.file = r[BOM_FILE]
            args.format = r[BOM_FORMAT]
            args.project = r[BOM_PROJECT]
            try:
                f = check_dir_file(args)
            except CheckDirError as e:
                msg.msg(str(e))
                continue
            files += f
        if not files:
            msg.reimport_missing_file()
            sys.exit(1)
        args.overwrite = True
    return list(set(files))


def tab_info(tab: str, silent: bool = False) -> list[str]:
    """diplay info about columns in BOM table"""
    try:
        must_col, nice_col = tab_cols(tab,all_cols=True)
    except SqlTabError as err:
        msg.msg(str(err))
        sys.exit(1)
    must_col.sort()
    nice_col.sort()
    if not silent:
        msg.bom_info(must_col, nice_col, col_description())
    return must_col + nice_col


def tab_template(tab: str, args: Namespace) -> None:
    """save csv tempalete to a file"""
    cols = pd.Series(tab_info(tab=tab, silent=True))
    csv = pd.DataFrame(columns=cols)
    csv.to_csv(args.csv_template, index=False)
    msg.msg(f"template written to {args.csv_template}")


def col_description() -> dict:
    """extract columns descriptions from sql_scheme"""
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    col_desc = {}
    for _, cont in sql_scheme.items():
        if "COL_DESCRIPTION" in cont:
            for k, v in cont["COL_DESCRIPTION"].items():
                if k not in col_desc:
                    col_desc[k] = v
    return col_desc
