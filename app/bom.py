"""
BOM tools:
    - import from file
    - remove
    - list
"""

import os
from argparse import Namespace

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from app import sql
from app.common import (
    BOM_PROJECT,
    IMPORT_FORMAT_SPECIAL_KEYS,
    NO_EXPORT_COLS,
)
from app.message import MessageHandler
from app.tabs import (
    bom_info,
    bom_template,
    import_tab,
    prepare_project,
    scan_files,
)
from conf.config import import_format

msg = MessageHandler()


def bom_import(args: Namespace) -> None:
    """
    import BOM from file
    """
    if args.info:
        bom_info(tab="BOM")
        return
    if args.remove:
        bom_remove(args)
        return
    if args.csv_template:
        bom_template(args=args, tab="BOM")
        return
    if args.export:
        bom_export(args)
        return

    files = scan_files(args)
    for file in files:
        new_stock = import_file(args, file)
        if new_stock.empty:
            continue
        import_tab(
            dat=new_stock,
            tab="BOM",
            args=args,
            file=file,
        )


def bom_remove(args: Namespace) -> None:
    """
    Remove from BOM table based on match on project column
    Remove only not commited projects.
    """
    if (projects := prepare_project(projects=args.remove, commited=False)) == []:
        return
    sql.rm(tab="BOM", value=projects, column=[BOM_PROJECT])
    msg.bom_remove(projects)


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
    df.drop(columns=NO_EXPORT_COLS, inplace=True, errors="ignore")
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
