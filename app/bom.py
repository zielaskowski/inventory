import os
import pandas as pd
from pandas.errors import ParserError
from argparse import Namespace

from app.sql import getL, getDF, rm, put
from app.common import check_dir_file, read_json
from app.tabs import columns_align, prepare_tab
from conf.config import import_format, SQL_scheme
from app.error import messageHandler, prepare_tabError, check_dirError

msg = messageHandler()


def bom_import(
    args: Namespace,
) -> None:
    if args.clean:
        # remove all old data
        rm(tab="BOM")
        print("Removed all data from BOM table")
        return

    if not args.reimport:
        try:
            xls_files = check_dir_file(args)
        except check_dirError as e:
            print(e)
            exit(1)
    else:
        xls_files = getDF(tab="BOM", get=["dir", "file"])
        xls_files["path"] = xls_files["dir"] + "/" + xls_files["file"]
        xls_files = xls_files["path"].tolist()

    new_stock = pd.DataFrame()
    imported_files = []

    # import all xlsx files
    for file in xls_files:
        # go through all files and append to dataframe
        msg.import_file(file)
        try:
            new_stock = pd.read_excel(
                file,
                **{
                    k: v
                    for k, v in import_format[args.format].items()
                    if k
                    not in [
                        "cols",
                        "dtype",
                    ]
                },
            )
        except ParserError as e:
            print("Possibly wrong excel format (different shop?)")
            print(e)
            continue
        except ValueError as e:
            print("Possibly 'no matched' row.")
            print(e)
            continue
        except FileNotFoundError as e:
            print(e)
            continue

        old_files = getL(tab="BOM", get=["file"])
        if args.overwrite:
            # remove all old data
            rm(tab="BOM", value=[os.path.basename(file)], column=["file"])
        else:
            if os.path.basename(file) in old_files:
                if not msg.file_already_imported(os.path.basename(file)):
                    continue

        # rename (and tidy) columns according to format of imported file
        new_stock = columns_align(
            new_stock.copy(),
            file=file,
            supplier=args.format,
        )

        # write data to SQL
        # need to start from DEVICE becouse other tables refer to it
        try:
            tabs, dat = prepare_tab(
                        dat=new_stock.copy(),
                        tabs=["DEVICE",'BOM','SHOP'],
                        qty=args.qty,
                        file=file,
                        row_shift=import_format[args.format]["header"],
                        )
            
            # put into SQL
            sql_scheme = read_json(SQL_scheme)
            for tab in tabs:
                put(
                    dat=dat,
                    tab=tab,
                    on_conflict=sql_scheme[tab]["ON_CONFLICT"],
                )
        except prepare_tabError as e:
            print(e)
            continue
        imported_files.append(file)

    # summary
    msg.BOM_import_summary(imported_files, new_stock)
