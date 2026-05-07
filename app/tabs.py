"""
helper function for importing
clean and tidy tables, but also manage files scanning
"""

import hashlib
import os
import re
import sys
from argparse import Namespace
from datetime import date

import pandas as pd
from pandas.core.dtypes.cast import NAType

import conf.config as conf
from app import sql
from app.common import (
    check_dir_file,
    foreign_tabs,
    match_from_list,
    read_json_dict,
    tab_cols,
    unpack_foreign,
)
from app.error import (
    AmbigousMatchError,
    CheckDirError,
    NoMatchError,
    PrepareTabError,
    ScanDirPermissionError,
    SqlTabError,
)
from app.manufacturers import (
    align_other_cols,
    find_alt_man,
    use_alt_man,
)
from app.message import msg


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
    ex_devs = sql.getL(tab="DEVICE", get_col=[conf.DEV_HASH])

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
    dat.loc[:, conf.DEV_MAN], _ = use_alt_man(dat[conf.DEV_MAN].to_list())

    # hash columns - must be last so all columns aligned and present
    dat = hash_tab(dat=dat)

    # align other cols before skipping data in incoming data
    on_conflict =  {"action": "IGNORE"}
    if not args.dont_align_columns:
        on_conflict = None  # will take default: UPDATE_SET
        existing_data = sql.getDF(
            tab="DEVICE",
            search=dat.loc[:, conf.DEV_HASH],
            where=[conf.DEV_HASH],
        )
        try:
            miss_cols = existing_data.columns.difference(dat.columns)
            dat[miss_cols] = pd.DataFrame(
                None, index=dat.index, columns=miss_cols, dtype=object
            )
            existing_data["dev_rm"] = existing_data[conf.DEV_ID]
            existing_data["man_rm"] = existing_data[conf.DEV_MAN]
            dat = align_other_cols(rm_dat=existing_data, keep_dat=dat)
        except ValueError as e:
            # wrong input for align_other_cols()
            print(e)
        except KeyError:
            # no existing data
            pass
        except KeyboardInterrupt as e:
            print(e)
            sys.exit(1)
    # check if data already in sql
    if tab == "BOM" and not check_existing_project(dat, args):
        return  # user do not want to overwrite nor add to existing data
    if tab == "STOCK" and not check_existing_data(dat, args):
        return

    # check for alternative manufacturer on the same dev_id
    # just inform that alignment can be done with admin functions
    find_alt_man(
        dat=dat.copy(deep=True),
        just_inform=True,
    )

    # inform if data useful for other tabs is present
    tabs = tabs_in_data(dat)
    if "SHOP" in tabs and tab != "SHOP":
        msg.msg("Detected data usefull also for SHOP table.")
        msg.msg("Consider importing with 'shop_cart_import' option.")
    # write aligned data to SQL
    for t in ["DEVICE", tab]:
        if t == "DEVICE":
            sql.put(dat=dat, tab=t, on_conflict=on_conflict)
        else:
            sql.put(dat=dat, tab=t)

    # SUMMARY
    if tab == "BOM":
        msg.bom_import_summary(dat, len(dat[dat[conf.BOM_HASH].isin(ex_devs)]))


def tabs_in_data(dat: pd.DataFrame) -> list[str]:
    """
    return all tables which mandatory cols are present in dat
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
    if conf.BOM_PROJECT in missing_cols:
        dat[conf.BOM_PROJECT] = dat[conf.BOM_FILE].apply(
            lambda cell: cell.split(".")[0]
        )
        missing_cols = [c for c in missing_cols if c != conf.BOM_PROJECT]
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


def NA_rows(  # pylint: disable=invalid-name
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
    n_stock = n_stock.astype(object)
    if dtype := conf.import_format[args.format].get("dtype"):
        exist_col_dtypes = {k: v for k, v in dtype.items() if k in n_stock.columns}
        try:
            n_stock = n_stock.astype(exist_col_dtypes)
        except ValueError as e:
            msg.msg(str(e))
            sys.exit(1)

    # Strip whitespace from all string elements in the DataFrame
    n_stock = n_stock.map(lambda x: x.strip() if isinstance(x, str) else x)

    # add column with path and file name and supplier
    n_stock[conf.BOM_DIR] = os.path.abspath(args.dir)
    n_stock[conf.BOM_FILE] = os.path.basename(file)
    n_stock[conf.BOM_FORMAT] = args.format
    n_stock[conf.SHOP_DATE] = date.today().strftime("%Y-%m-%d")
    if conf.SHOP_SHOP not in n_stock.columns:
        n_stock[conf.SHOP_SHOP] = conf.import_format[args.format].get(
            "shop", args.format
        )

    return n_stock


def ASCII_txt(  # pylint: disable=invalid-name
    txt: str | None | NAType,
) -> str | None | NAType:  # pylint: disable=invalid-name
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


def align_data(dat: pd.DataFrame) -> pd.DataFrame:
    """
    align new data with existing:
        1. align manufacturers
        2. where manufacturer changed, align all other columns
        3. remove devs where manufacturer changed
        4. return aligned data to be added
    return DataFrame with changed devices with additional column 'dev_rm'
    with device hashes before change
    raise  VimdiffSelError when user abort or mess with vimdiff
    """
    align_dat = find_alt_man(
        dat.copy(deep=True),
        just_inform=False,
    )
    # select rows where change
    differ_row = dat[conf.DEV_MAN] != align_dat[conf.DEV_MAN]
    # create new hash after possibly changing manufacturer
    align_dat = hash_tab(dat=align_dat).loc[differ_row, :]
    # if all differ_row is False, no change (data aligned or align manufacture did the job)
    if align_dat.empty:
        msg.msg("Data is aligned. Nothing done.")
        return align_dat
    # keep new device in separate DataFrame
    keep_dev_hash = align_dat[conf.DEV_HASH].drop_duplicates().to_list()
    keep_dev = dat[dat[conf.DEV_HASH].isin(keep_dev_hash)]
    if len(keep_dev) != len(keep_dev_hash):
        # new manufacturer name somewhere, device not existing in db
        new_dev_hash = [c for c in keep_dev_hash if c not in dat[conf.DEV_HASH]]
        keep_dev = pd.concat(
            [
                keep_dev,
                align_dat[align_dat[conf.DEV_HASH].isin(new_dev_hash)].drop_duplicates(
                    subset=[conf.DEV_HASH]
                ),
            ],
        )
    # possibly empty if no differ rows
    if not any(differ_row.to_list()):
        return dat
    # mark devs that will be removed (dev_rm and man_rm)
    align_dat = pd.concat(
        [
            dat.loc[differ_row, [conf.DEV_HASH, conf.DEV_MAN]].rename(
                columns={
                    conf.DEV_HASH: "dev_rm",
                    conf.DEV_MAN: "man_rm",
                }
            ),  # fmt: ignore
            align_dat,
        ],
        axis="columns",
    )  # fmt: ignore
    # align all other dev cols
    keep_dev = align_other_cols(rm_dat=align_dat, keep_dat=keep_dev)  # pyright: ignore
    # add data from other tabs for devs we are going to remove
    # don't take old hashes, but rehash again
    keep_dev = pd.merge(
        left=keep_dev,
        right=align_dat.loc[:, ["dev_rm", conf.DEV_HASH]],
        on=conf.DEV_HASH,
        how="right",
    )
    keep_dev = sql.getDF_other_tabs(
        dat=keep_dev,
        hash_list=keep_dev["dev_rm"].to_list(),
        merge_on="dev_rm",
    )
    keep_dev = hash_tab(keep_dev)
    return keep_dev


def check_existing_project(dat: pd.DataFrame, args: Namespace) -> bool:
    """
    check if project already present in BOM
    if -overwrite, remove existing data
    other way ask for confirmation
    return True if we can continue
    """
    old_project = sql.getL(tab="BOM", get_col=[conf.BOM_PROJECT])
    project = dat.loc[0, conf.BOM_PROJECT]
    if args.overwrite:
        # remove all old data
        sql.rm(
            tab="BOM",
            value=dat[conf.BOM_PROJECT].to_list(),
            column=[conf.BOM_PROJECT],
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
    old_data = sql.getL(tab="STOCK", get_col=[conf.STOCK_HASH])
    overlap_data = dat.loc[dat[conf.STOCK_HASH].isin(old_data), :]
    if overlap_data.empty:
        # no existing data so -overwrite dosent make sense
        args.overwrite = False
        return True
    if args.overwrite:
        # remove all old data
        sql.rm(
            tab="STOCK",
            value=overlap_data[conf.STOCK_HASH].to_list(),
            column=[conf.STOCK_HASH],
        )
        return True
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
        get_col=[conf.BOM_PROJECT],
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
            get_col=[conf.BOM_DIR, conf.BOM_FILE, conf.BOM_PROJECT, conf.BOM_FORMAT],
        )
        if locations.empty:
            msg.reimport_missing_file()
            sys.exit(1)
        files = []
        for _, r in locations.iterrows():
            args.dir = r[conf.BOM_DIR]
            args.file = r[conf.BOM_FILE]
            args.format = r[conf.BOM_FORMAT]
            args.project = r[conf.BOM_PROJECT]
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
        must_col, nice_col = tab_cols(tab, all_cols=True)
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
