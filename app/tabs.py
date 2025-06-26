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
    DEV_HASH,
    DEV_ID,
    DEV_MAN,
    SHOP_DATE,
    SHOP_HASH,
    SHOP_SHOP,
    STOCK_HASH,
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
    AmbigousMatchError,
    CheckDirError,
    NoMatchError,
    PrepareTabError,
    ScanDirPermissionError,
    SqlTabError,
)
from app.message import MessageHandler
from conf import config as conf

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

    # if 'price' in columns and device_id
    if "price" in dat.columns and tab == "BOM":
        print('Add shop cart with "shop_cart_import" command.')
        print("skiping this file.")
        return

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

    # check if data already in sql, only for BOM tab
    if tab == "BOM" and not check_existing_data(dat, args, file):
        return  # user do not want to overwrite nor add to existing data

    # hash columns - must be last so all columns aligned and present
    dat = hash_tab(tab=tab, dat=dat)

    sql_scheme = read_json_dict(conf.SQL_SCHEME)

    # check for different manufacturer on the same dev_id
    aligned_dat = align_data(dat)
    # possibly return data from other tabs (if manufacturers are to be aligned)
    try:
        no_diff = True
        pd.testing.assert_frame_equal(
            aligned_dat,
            dat,
            check_dtype=False,
        )
    except AssertionError:
        no_diff = False
    if no_diff:
        tabs = foreign_tabs(tab) + [tab]
    else:
        tabs = sorted(list(sql_scheme.keys()), key=lambda x: (x != "DEVICE", x))

    # write data to SQL
    # need to start from DEVICE because other tables refer to it
    for t in tabs:
        sql.put(
            dat=aligned_dat,
            tab=t,
            on_conflict=sql_scheme[t].get("ON_CONFLICT", {}),
        )

    # SUMMARY
    if tab == "BOM":
        msg.bom_import_summary(dat, len(dat[dat[BOM_HASH].isin(ex_devs)]))


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
        raise PrepareTabError(tab, file, missing_cols)

    # remove rows with NA in must_cols
    dat = NA_rows(dat, must_cols, nice_cols, row_shift)

    # clean text, leave only ASCII
    # i.e. easyEDM writes manufactuere in Chinese in parenthesis
    # MUST be after NA_rows, becouse ASCI_txt affect NaNs
    for c in dat.columns:
        if dat[c].dtype == "object":
            dat[c] = dat[c].apply(ASCII_txt)

    # and strip text
    dat = dat.apply(lambda x: x.str.strip() if x.dtype == object else x)  # type: ignore

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

    sql_scheme = read_json_dict(conf.SQL_SCHEME)
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
    row_shift += 2  # one for header, and one to start from zero
    na_rows = df.loc[df.loc[:, must_cols].isna().any(axis=1)]
    na_rows_id: list[int] = [int(c) + row_shift for c in na_rows.index.values]
    df = df.loc[~df.index.isin(na_rows.index)]

    # check for nice cols
    nicer_cols = [c for c in nice_cols if c in df.columns]
    na_rows = df.loc[df.loc[:, nicer_cols].isna().any(axis=1)]
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
    n_stock[BOM_COMMITED] = False
    n_stock[BOM_DIR] = os.path.abspath(args.dir)
    n_stock[BOM_FILE] = os.path.basename(file)
    n_stock[BOM_FORMAT] = args.format
    n_stock[SHOP_DATE] = date.today().strftime("%Y-%m-%d")
    if SHOP_SHOP not in n_stock.columns:
        n_stock[SHOP_SHOP] = args.format

    return n_stock


def ASCII_txt(txt: str) -> str:
    """remove any chinese signs from string columns"""
    txt = str(txt).encode("ascii", "ignore").decode("ascii")
    # remove any empty paranthases '()' from string columns
    txt = re.sub(r"\(.*?\)", "", txt)
    return txt


def align_data(dat: pd.DataFrame) -> pd.DataFrame:
    """
    align new data with existing:
        1. align manufacturers
        2. where manufacturer changed, align all other columns
        3. remove devs where manufacturer changed
        4. return aligned data to be added
    """
    align_dat = dat.copy(deep=True)
    man_cols = [DEV_ID, DEV_MAN]
    while True:
        temp_dat = align_manufacturers(align_dat.copy(deep=True))
        try:
            pd.testing.assert_frame_equal(
                align_dat.loc[:, man_cols],
                temp_dat.loc[:, man_cols],
                check_dtype=False,
            )
        except AssertionError:
            align_dat = temp_dat
            continue
        break
    align_dat = hash_tab(tab="DEVICE", dat=align_dat)
    differ_row = dat[DEV_MAN] != align_dat[DEV_MAN]
    dev_align = pd.DataFrame(
        {
            "dev_rm": dat.loc[differ_row, DEV_HASH],
            "man_rm": dat.loc[differ_row, DEV_MAN],
            DEV_HASH: align_dat.loc[differ_row, DEV_HASH],
            DEV_MAN: align_dat.loc[differ_row, DEV_MAN],
        }
    )
    if dev_align.empty:
        return dat
    # align all other dev cols
    dat_align = align_other_cols(dat=dev_align)
    # add data from other tabs for devs we are going to remove
    new_dat = pd.concat(
        [
            dat_align,
            sql.getDF(
                tab="BOM",
                search=dev_align.loc[:, "dev_rm"],
                where=[BOM_HASH],
            ),
            sql.getDF(
                tab="SHOP",
                search=dev_align.loc[:, "dev_rm"],
                where=[SHOP_HASH],
            ),
            sql.getDF(
                tab="STOCK",
                search=dev_align.loc[:, "dev_rm"],
                where=[STOCK_HASH],
            ),
        ],
        axis="columns",
    )
    new_dat = new_dat.ffill()
    for tab in ["BOM", "SHOP", "STOCK", "DEVICE"]:
        sql.rm(tab=tab, value=dev_align.loc[:, "dev_rm"])
    return new_dat


def align_other_cols(dat: pd.DataFrame) -> pd.DataFrame:
    """
    expect DataFrame with one pair replacement per row:
    and columns: dev_rm | man_rm | DEV_HASH | DEV_MAN
    display all other attributes of device to user to choose
    dispalay only not empty and attributes that differ
    return selected attributes attached in row to input dat
    """
    necessery_cols = ["dev_rm", "man_rm", DEV_HASH, DEV_MAN]
    if not all(c in necessery_cols for c in dat.columns):
        sys.exit(1)
    must_cols, nice_cols = tab_cols(tab="DEVICE")
    display_cols = [
        c for c in must_cols + nice_cols if c not in [DEV_HASH, DEV_MAN, DEV_ID]
    ]
    dat_align = pd.DataFrame(
        columns=pd.Series(list(set(must_cols + nice_cols + necessery_cols)))
    )
    for _, r in dat.iterrows():
        add_attr = (
            sql.getDF(
                tab="DEVICE",
                search=[getattr(r, "dev_rm")],
                where=[DEV_HASH],
            )
            .loc[:, display_cols]
            .dropna(axis="columns")
        )
        rm_attr = (
            sql.getDF(
                tab="DEVICE",
                search=[getattr(r, DEV_HASH)],
                where=[DEV_HASH],
            )
            .loc[:, display_cols]
            .dropna(axis="columns")
        )
        # if NA in add_attr (missing col), take from rm_attr if present
        for c in rm_attr.columns:
            if c not in add_attr:
                add_attr[c] = rm_attr[c]
        add_attr_cols = list(add_attr.columns)
        # hide attributes which are already aligned
        for c in add_attr.columns:
            if add_attr[c].to_string(index=False) == rm_attr[c].to_string(index=False):
                add_attr_cols.remove(c)
        if not add_attr_cols:
            aligned_cols = add_attr.iloc[0, :].to_list()
        else:
            aligned_cols = vimdiff_selection(
                ref_col={"column": add_attr_cols},
                change_col={str(r["man_rm"]): add_attr.iloc[0, :].to_list()},
                opt_col={str(r[DEV_MAN]): rm_attr.iloc[0, :].to_list()},
                exit_on_change=False,
            )
        dat_align = pd.concat(
            [
                dat_align,
                pd.DataFrame(
                    [aligned_cols + r.to_list()],
                    columns=add_attr.columns.to_list() + necessery_cols,
                ),
            ],
            ignore_index=True,
        )
    return dat_align


def align_manufacturers(dat: pd.DataFrame) -> pd.DataFrame:
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
    """
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

    # if we have stored alternatives, use it
    dat_dup.loc[:, DEV_MAN] = get_alternatives(dat_dup[DEV_MAN].to_list())

    # remove dup_col from dup_col_grp
    for r in dat_dup.itertuples():
        dup_col_grp_v = getattr(r, man_grp_col)
        dup_col_v = getattr(r, DEV_MAN)
        opts = str(dup_col_grp_v).split(" | ")
        opts = [o for o in opts if o != dup_col_v]
        dat_dup.loc[r.Index, man_grp_col] = " | ".join(opts)

    chosen = vimdiff_selection(
        ref_col={"devices": dat_dup.loc[:, DEV_ID].to_list()},
        change_col={DEV_MAN: dat_dup.loc[:, DEV_MAN].to_list()},
        opt_col={man_grp_col: dat_dup.loc[:, man_grp_col].to_list()},
        exit_on_change=True,
    )
    dat_dup[DEV_MAN] = chosen
    # put new data into orginal tab, based on preserved indexes
    dat.loc[dat_dup.index, DEV_MAN] = dat_dup[DEV_MAN]

    return dat


def vimdiff_selection(
    ref_col: dict[str, list[str]],
    change_col: dict[str, list[str]],
    opt_col: dict[str, list[str]],
    exit_on_change: bool,
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
        with open(
            conf.TEMP_DIR + cols[key] + ".txt",
            mode="w",
            encoding="UTF8",
        ) as f:
            for ind, item in enumerate(cols[key + 1]):
                f.write(str(ind) + "| " + item + "\n")  # add line number: '1| txt'
                # other way diff may shift rows to align data between columns

    vimdiff_config(
        ref_col=cols[ref_k],
        change_col=cols[change_k],
        opt_col=cols[opt_k],
        alternate_col=cols[change_k],
        exit_on_change=exit_on_change,
    )

    vim_cmd = "vim -u " + conf.TEMP_DIR + ".vimrc"
    if conf.DEBUG == "none":
        subprocess.run(vim_cmd, shell=True, check=False)
    elif conf.DEBUG == "debugpy":
        with subprocess.Popen("konsole -e " + vim_cmd, shell=True) as p:
            p.wait()

    with open(conf.TEMP_DIR + cols[change_k] + ".txt", mode="r", encoding="UTF8") as f:
        chosen = f.readlines()

    # remove line numbers
    chosen = [re.sub(r"^\d+\|\s*", "", c) for c in chosen]
    chosen = [c.strip() for c in chosen]
    if any(len(chosen) != len(cols[v]) for v in [ref_v, change_v, opt_v]):
        # if user mess-up and added/removed rows
        return cols[change_v]
    # write 1to1 matches selected by user, so next time save some time
    if DEV_MAN == cols[ref_k]:
        store_alternatives(
            alternatives={
                cols[change_k]: cols[change_v],
                cols[opt_k]: cols[opt_v],
            },
            selection=chosen,
        )
    return chosen


def check_existing_data(dat: pd.DataFrame, args: Namespace, file: str) -> bool:
    """
    check if data already present in sql
    if -overwrite, remove existing data
    other way ask for confirmation
    return True if we can continue
    """
    file_name = os.path.basename(file)
    old_files = sql.getL(tab="BOM", get_col=[BOM_FILE])
    old_project = sql.getL(tab="BOM", get_col=[BOM_PROJECT])
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
        get_col=[BOM_PROJECT],
        search=commit_search,
        where=[BOM_COMMITED],
    )
    all_projects = sql.getL(
        tab="BOM",
        get_col=[BOM_PROJECT],
    )
    if projects == ["?"]:
        msg.bom_prepare_projects(
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
        except AmbigousMatchError as err:
            print(err)
        except NoMatchError as err:
            print(err)

    if not any(p in available for p in match_projects):
        msg.bom_prepare_projects(
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
            search=["False"],
            where=[BOM_COMMITED],
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


def bom_info(tab: str, silent: bool = False) -> list[str]:
    """diplay info about columns in BOM table"""
    try:
        must_col, nice_col = tab_cols(tab)
    except SqlTabError as err:
        msg.msg(str(err))
        sys.exit(1)
    if not silent:
        msg.bom_info(must_col, nice_col, col_description())
    return must_col + nice_col


def bom_template(tab: str, args: Namespace) -> None:
    """save csv tempalete to a file"""
    cols = pd.Series(bom_info(tab=tab, silent=True))
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
