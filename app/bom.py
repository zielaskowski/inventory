"""
BOM tools:
    - import from file
    - remove
    - list
"""

import os
import sys
from argparse import Namespace

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from app import sql
from app.common import (
    BOM_COMMITED,
    BOM_DIR,
    BOM_FILE,
    BOM_FORMAT,
    BOM_PROJECT,
    DEV_ID,
    IMPORT_FORMAT_SPECIAL_KEYS,
    NO_EXPORT_COLS,
    check_dir_file,
    foreign_tabs,
    read_json,
    tab_cols,
)
from app.error import (
    check_dirError,
    messageHandler,
    prepare_tabError,
    scan_dir_permissionError,
    sql_tabError,
)
from app.tabs import check_existing_data, columns_align, prepare_project, prepare_tab
from conf.config import SQL_SCHEME, import_format

msg = messageHandler()


def bom_import(args: Namespace) -> None:
    """
    import BOM from file
    """
    if args.info:
        bom_info()
        return
    if args.remove:
        bom_remove(args)
        return
    if args.csv_template:
        bom_template(args)
        return
    if args.export:
        bom_export(args)
        return

    files = scan_files(args)
    for file in files:
        new_stock = import_file(args, file)
        if new_stock.empty:
            continue

        # rename (and tidy) columns according to format of imported file
        # apply configuration from config.py
        new_stock = columns_align(
            new_stock.copy(),
            file=file,
            args=args,
        )

        # if 'price' in columns and device_id
        if "price" in new_stock.columns:
            print('Add shop cart with "cart" command.')
            print("skiping this file.")
            continue

        # existing device for summary info reasons
        ex_devs = sql.getL(tab="BOM", get=[DEV_ID], follow=True)

        try:
            # align table with sql definition
            # remove NAs in mandatory columns
            # hash columns
            dat = prepare_tab(
                dat=new_stock.copy(),
                tab="BOM",
                file=file,
                row_shift=import_format[args.format]["header"],
            )
        except prepare_tabError as e:
            msg.msg(str(e))
            continue

        # check if data already in sql
        if not check_existing_data(dat, args, file):
            continue  # user do not want to overwrite nor add to existing data

        # write data to SQL
        # need to start from DEVICE because other tables refer to it
        sql_scheme = read_json(SQL_SCHEME)
        for tab in foreign_tabs("BOM") + ["BOM"]:
            sql.put(
                dat=dat,
                tab=tab,
                on_conflict=sql_scheme[tab].get("ON_CONFLICT", {}),
            )

        # SUMMARY
        msg.BOM_import_summary(
            new_stock, len(new_stock[new_stock[DEV_ID].isin(ex_devs)])
        )


def bom_remove(args: Namespace) -> None:
    """
    Remove from BOM table based on match on project column
    Remove only not commited projects.
    """
    if (projects := prepare_project(projects=args.remove, commited=False)) == []:
        return
    sql.rm(tab="BOM", value=projects, column=[BOM_PROJECT])
    msg.BOM_remove(projects)


def import_csv(file: str) -> pd.DataFrame:
    """import csv using format described in config.py"""
    msg.import_file(file)
    try:
        new_bom = pd.read_csv(
            file,
            **{
                k: v
                for k, v in import_format["csv"].items()
                if k not in IMPORT_FORMAT_SPECIAL_KEYS
            },
        )
    except ParserError as err:
        msg.unknown_import(err)
        return pd.DataFrame()
    except EmptyDataError as err:
        msg.unknown_import(err)
        return pd.DataFrame()
    return new_bom.copy()


def import_xls(file: str, file_format: str) -> pd.DataFrame:
    """
    import excel using format described in config.py
    based on file source format provide args and kwargs
    of read_excel() function
    """
    msg.import_file(file)
    try:
        new_bom = pd.read_excel(
            file,
            **{
                k: v
                for k, v in import_format[file_format].items()
                if k not in IMPORT_FORMAT_SPECIAL_KEYS
            },
        )
    except ParserError as e:
        msg.unknown_import(e)
        return pd.DataFrame()
    except EmptyDataError as err:
        msg.unknown_import(err)
        return pd.DataFrame()
    except ValueError as e:
        msg.unknown_import(e)
        return pd.DataFrame()
    except FileNotFoundError as e:
        msg.unknown_import(e)
        return pd.DataFrame()
    return new_bom.copy()


def scan_files(args) -> list[str]:
    """scan for files to be imported for BOM"""
    if not args.reimport:
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


def bom_info(silent: bool = False) -> list[str]:
    """diplay info about columns in BOM table"""
    try:
        must_col, nice_col = tab_cols("BOM")
    except sql_tabError as err:
        msg.msg(str(err))
        sys.exit(1)
    if not silent:
        msg.BOM_info(must_col, nice_col)
    return must_col + nice_col


def bom_template(args: Namespace) -> None:
    """save csv tempalete to a file"""
    cols = pd.Series(bom_info(silent=True))
    csv = pd.DataFrame(columns=cols)
    csv.to_csv(args.csv_template, index=False)
    msg.msg(f"template written to {args.csv_template}")


def import_file(args: Namespace, file: str) -> pd.DataFrame:
    """import files"""
    if args.format == "csv":
        return import_csv(file)
    return import_xls(file, args.format)


def bom_export(args: Namespace) -> None:
    """print or export data in BOM table"""
    if (projects := prepare_project(args.export, commited=False)) == []:
        return
    df = sql.getDF(tab="BOM", search=projects, where=[BOM_PROJECT], follow=True)
    df.drop(columns=NO_EXPORT_COLS, inplace=True)
    if args.hide_columns:
        cols = [c for c in args.hide_columns if c in df.columns]
        df.drop(columns=cols, inplace=True)
    if not args.file:
        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.width",
            500,
        ):
            print(df)
        return
    df.to_csv(os.path.join(args.dir, args.file), index=False)
