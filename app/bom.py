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
from pandas.errors import ParserError

from app.common import check_dir_file, read_json
from app.error import check_dirError, messageHandler, prepare_tabError
from app.sql import getDF, getL, put, rm
from app.tabs import columns_align, prepare_tab
from conf.config import SQL_SCHEME, import_format

msg = messageHandler()


def bom_import(
    args: Namespace,
) -> None:
    """
    import BOM from file
    """
    if args.remove:
        bom_remove(args)
        return

    if not args.reimport:
        try:
            xls_files = check_dir_file(args)
        except check_dirError as e:
            print(e)
            sys.exit(1)
    else:
        xls_files = getDF(tab="BOM", get=["dir", "file"])
        xls_files["path"] = xls_files["dir"] + "/" + xls_files["file"]
        xls_files = xls_files["path"].tolist()
        args.overwrite = True

    for file in xls_files:
        new_stock = import_xls(file, args.format)
        if new_stock.empty:
            continue

        file_name = os.path.basename(file)
        old_files = getL(tab="BOM", get=["file"])
        if args.overwrite:
            # remove all old data
            rm(tab="BOM", value=[file_name], column=["file"])
        else:
            if file_name in old_files:
                if not msg.file_already_imported(file_name):
                    continue

        # rename (and tidy) columns according to format of imported file
        new_stock = columns_align(
            new_stock.copy(),
            file=file,
            supplier=args.format,
        )
        # if 'price' in columns and device_id
        if "price" in new_stock.columns:
            print('Add shop cart with "cart" command.')
            print("skiping this file.")
            continue

        # existing device for summary info reasons
        ex_devs = getL(tab="BOM", get=["device_id"])

        # write data to SQL
        # need to start from DEVICE because other tables refer to it
        try:
            tabs, dat = prepare_tab(
                dat=new_stock.copy(),
                tabs=["DEVICE", "BOM"],
                file=file,
                row_shift=import_format[args.format]["header"],
            )
        except prepare_tabError as e:
            print(e)
            continue

        # put into SQL
        sql_scheme = read_json(SQL_SCHEME)
        for tab in tabs:
            put(
                dat=dat,
                tab=tab,
                on_conflict=sql_scheme[tab]["ON_CONFLICT"],
            )

        # SUMMARY
        msg.BOM_import_summary(
            new_stock, len(new_stock[new_stock["device_id"].isin(ex_devs)])
        )


def bom_remove(args: Namespace) -> None:
    """
    Remove from BOM table based on match on file column
    """
    bom = getDF(tab="BOM")
    if not bom.empty:
        if args.file is not None:
            bom = bom[bom["file"].str.contains(args.file)]

        rm(tab="BOM", value=bom["device_id"].tolist(), column=["device_id"])
        msg.BOM_remove(args.file)


def import_xls(file: str, file_format: str) -> pd.DataFrame:
    """
    import excel using format described in config
    dased on file source format provide args and kwargs
    of read_excel() function
    """
    msg.import_file(file)
    try:
        new_bom = pd.read_excel(
            file,
            **{
                k: v
                for k, v in import_format[file_format].items()
                if k not in ["cols", "dtype", "func"]
            },
        )
    except ParserError as e:
        msg.unknown_import(e)
        return pd.DataFrame()
    except ValueError as e:
        print("Possibly 'no matched' row.")
        print(e)
        return pd.DataFrame()
    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()
    return new_bom.deepcopy()
