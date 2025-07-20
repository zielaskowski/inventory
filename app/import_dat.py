"""
import/export tools for tables (BOM,SHOP,STOCK)
also commits
"""

import os
import sys
from argparse import Namespace

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

import conf.config as conf
from app import sql
from app.common import (
    IMPORT_FORMAT_SPECIAL_KEYS,
    NO_EXPORT_COLS,
)
from app.message import MessageHandler
from app.tabs import (
    import_tab,
    prepare_project,
    scan_files,
    tab_info,
    tab_template,
)
from conf.config import COL_WIDTH, import_format
from conf.sql_colnames import *

msg = MessageHandler()


def stock_import(args: Namespace) -> None:
    """Main stock function"""
    if args.add_project:
        commit_project(args=args)
        return
    if args.use_project or args.use_device_id or args.use_device_manufacturer:
        use_stock(args=args)
        return
    if args.info:
        tab_info(tab="STOCK")
        return
    if args.csv_template:
        tab_template(args=args, tab="STOCK")
        return
    if args.export or args.fzf:
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
        if (projects := prepare_project(args.export)) == []:
            return
        df = sql.getDF(tab=tab, search=projects, where=[BOM_PROJECT], follow=True)
        cols = conf.BOM_EXPORT_COL
    elif tab == "STOCK":
        df = sql.getDF(tab=tab, follow=True)
        # add shop id and projects
        df_shop = sql.getDF(tab="SHOP")
        df_bom = sql.getDF(tab="BOM")
        df = pd.merge(
            left=df,
            right=df_shop,
            left_on=DEV_HASH,
            right_on=SHOP_HASH,
            suffixes=("", "_drop"),
            how="left",
        )
        df = pd.merge(
            left=df,
            right=df_bom,
            left_on=DEV_HASH,
            right_on=BOM_HASH,
            suffixes=("", "_drop"),
            how="left",
        )
        df.drop(
            columns=[col for col in df.columns if col.endswith("_drop")], inplace=True
        )
        cols = conf.STOCK_EXPORT_COL + [SHOP_ID, BOM_PROJECT]
    else:
        df = sql.getDF(tab=tab, follow=True)
        cols = df.columns
    if df.empty:
        msg.msg(f"No data in table {tab}.")
        sys.exit(1)
    df.drop(columns=NO_EXPORT_COLS, inplace=True, errors="ignore")
    if args.export_columns:
        try:
            df = df[args.export_columns]
        except KeyError as e:
            msg.msg(str(e))
            msg.msg("Here columns info:")
            tab_info(tab)
            sys.exit(1)
    else:
        df = df[cols]
    if getattr(args, "fzf", False):
        file = conf.TEMP_DIR + "stock_export.csv"
        df.to_csv(
            file,
            columns=[c for c in cols if c != DEV_DESC] + [DEV_DESC],
            sep="|",
            index=False,
        )
        print(file)
        return
    if not args.file:

        def truncate(width):
            return lambda x: str(x)[:width] + (".." if len(str(x)) > width else "")

        formatter = {c: truncate(w) for c, w in COL_WIDTH.items() if c in df.columns}
        col_width = {c: w + 4 for c, w in COL_WIDTH.items() if c in df.columns}
        print(
            df.to_string(
                index=False,
                formatters=formatter,  # pyright: ignore
                col_space=col_width,  # pyright: ignore
            )
        )
        return
    df.to_csv(os.path.join(args.dir, args.file), index=False)


def commit_project(args: Namespace) -> None:
    """commit projects"""
    if (projects := prepare_project(projects=args.add_project)) == []:
        return
    dat = sql.getDF(
        tab="BOM",
        search=projects,
        where=[BOM_PROJECT],
    )
    dat.rename(columns={BOM_QTY: STOCK_QTY}, inplace=True)
    dat[STOCK_QTY] = dat[STOCK_QTY] * args.qty
    sql.put(dat, tab="STOCK")
    msg.stock_commit(projects)


def use_stock(args: Namespace) -> None:
    """
    remove devices from stock
    base on project or dev_id or dev_man
    """
    projects = []
    # prepare devices to use
    if args.use_project:
        if (projects := prepare_project(projects=args.use_project)) == []:
            return
        dat = sql.getDF(
            tab="BOM",
            search=projects,
            where=[BOM_PROJECT],
            follow=True,
        )
        dat["use_qty"] = dat[BOM_QTY] * args.qty
    else:
        dat = sql.getDF("DEVICE")
    if args.use_device_id:
        dat = dat.loc[dat[DEV_ID] == args.use_device_id, :]
        dat["use_qty"] = 1
    if args.use_device_manufacturer:
        dat = dat.loc[dat[DEV_MAN] == args.use_device_manufacturer, :]
        dat["use_qty"] = 1
    if dat.empty:
        msg.stock_use(
            dev_id=args.use_device_id,
            dev_man=args.use_device_manufacturer,
            no_devs=True,
        )
        return
    # collect stock
    stock = sql.getDF(tab="STOCK")
    if stock.empty:
        msg.stock_use(no_stock=True)
        return
    # merge and substract
    dat = pd.merge(
        left=dat,
        left_on=DEV_HASH,
        right=stock,
        right_on=STOCK_HASH,
        how="left",
    )
    dat[STOCK_QTY] = dat[STOCK_QTY] - dat["use_qty"]
    # do we have enough stock?
    dat_missing = dat.loc[(dat[STOCK_QTY] < 0) | (dat[STOCK_QTY].isna()), :]
    if not dat_missing.empty:
        if BOM_PROJECT in dat_missing.columns:
            missing_proj = dat_missing[BOM_PROJECT].unique()
        else:
            missing_proj = None
        msg.stock_use(
            project=missing_proj,
            dev_id=args.use_device_id,
            dev_man=args.use_device_manufacturer,
            not_enough=True,
        )
        return
    # remove what zeroed
    stock_end = dat.loc[dat[STOCK_QTY] == 0, :]
    if not stock_end.empty:
        sql.rm(tab="STOCK", value=stock_end[DEV_HASH], column=[STOCK_HASH])
        msg.stock_use(
            project=projects,
            dev_id=args.use_device_id,
            dev_man=args.use_device_manufacturer,
        )
        return
    new_stock = dat.loc[dat[STOCK_QTY] > 0, :]
    sql.edit(
        tab="STOCK",
        new_val=new_stock[STOCK_QTY].to_list(),
        col=STOCK_QTY,
        search=new_stock[DEV_HASH].to_list(),
        where=STOCK_HASH,
    )
    msg.stock_use(
        project=projects,
        dev_id=args.use_device_id,
        dev_man=args.use_device_manufacturer,
    )
