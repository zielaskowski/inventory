"""
import/export tools for tables (BOM,SHOP,STOCK)
also commits
"""

import os
import sys
from argparse import Namespace

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from app import sql
from app.common import (
    BOM_COMMITTED,
    BOM_PROJECT,
    BOM_QTY,
    IMPORT_FORMAT_SPECIAL_KEYS,
    NO_EXPORT_COLS,
    STOCK_QTY,
)
from app.message import MessageHandler
from app.tabs import (
    import_tab,
    prepare_project,
    scan_files,
    tab_info,
    tab_template,
)
from conf.config import import_format

msg = MessageHandler()


def stock_import(args: Namespace) -> None:
    """Main stock function"""
    if args.project:
        commit_project(args=args)
        return
    if args.info:
        tab_info(tab="STOCK")
        return
    if args.csv_template:
        tab_template(args=args, tab="STOCK")
        return
    if args.export:
        export(args, "STOCK")
        return
    files = scan_files(args)
    for file in files:
        new_stock = import_file(args, file)
        if new_stock.empty:
            continue
        import_tab(
            dat=new_stock,
            tab="STOCK",
            args=args,
            file=file,
        )


def shop_import(args: Namespace) -> None:
    """import shopping cart from file"""
    if args.info:
        tab_info(tab="SHOP")
        return
    if args.csv_template:
        tab_template(args=args, tab="SHOP")
        return
    if args.export:
        export(args, "SHOP")
        return

    files = scan_files(args)
    for file in files:
        new_stock = import_file(args, file)
        if new_stock.empty:
            continue
        import_tab(
            dat=new_stock,
            tab="SHOP",
            args=args,
            file=file,
        )


def bom_import(args: Namespace) -> None:
    """
    import BOM from file
    """
    if args.info:
        tab_info(tab="BOM")
        return
    if args.csv_template:
        tab_template(args=args, tab="BOM")
        return
    if args.export:
        export(args, "BOM")
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
    file_ext = import_format[args.format].get("file_ext", "")
    if "csv" in file_ext:
        return import_csv(file)
    if "xls" in file_ext or "xlsx" in file_ext:
        return import_xls(file, args.format)
    msg.msg(
        "Unknown file format. Make sure you have proper 'file_ext' value in 'config.py'"
    )
    sys.exit(1)


def export(args: Namespace, tab: str) -> None:
    """print or export data in BOM table"""
    if tab == "BOM":
        if (projects := prepare_project(args.export, committed=False)) == []:
            return
        df = sql.getDF(tab=tab, search=projects, where=[BOM_PROJECT], follow=True)
    else:
        df = sql.getDF(tab=tab, follow=True)
    df.drop(columns=NO_EXPORT_COLS, inplace=True, errors="ignore")
    if args.hide_columns:
        cols = [c for c in args.hide_columns if c in df.columns]
        df.drop(columns=cols, inplace=True)
    # replace 0/1 in commit column with True/False
    if BOM_COMMITTED in df.columns:
        df[BOM_COMMITTED] = df[BOM_COMMITTED].astype(bool)
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


def commit_project(args: Namespace) -> None:
    """commit projects"""
    if (projects := prepare_project(projects=args.project, committed=False)) == []:
        return
    dat = sql.getDF(
        tab="BOM",
        search=projects,
        where=[BOM_PROJECT],
    )
    sql.edit(
        tab="BOM",
        new_val=str(1),
        col=BOM_COMMITTED,
        search=projects,
        where=BOM_PROJECT,
    )
    dat.rename(columns={BOM_QTY: STOCK_QTY}, inplace=True)
    dat[STOCK_QTY] = dat[STOCK_QTY] * args.qty
    sql.put(dat, tab="STOCK")
    msg.bom_commit(projects)
