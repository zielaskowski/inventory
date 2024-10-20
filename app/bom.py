import os
import sys
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
    if args.remove:
        bom = getDF(tab="BOM")
        if not bom.empty:
            if args.file is not None:
                bom = bom[bom["file"].str.contains(args.file)]

            rm(tab="BOM", value=bom["device_id"].tolist(), column=["device_id"])
            msg.BOM_remove(args.file)
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
        args.overwrite = True

    new_stock = pd.DataFrame()

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
                        "func"
                    ]
                },
            )
        except ParserError as e:
            msg.unknown_import(e)
            continue
        except ValueError as e:
            print("Possibly 'no matched' row.")
            print(e)
            continue
        except FileNotFoundError as e:
            print(e)
            continue
        except:
            msg.unknown_import(sys.exc_info()[0])
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
        # if 'price' in columns and device_id
        if 'price' in new_stock.columns:
            print('Add shop cart with "cart" command.')
            print('skiping this file.')
            continue

        # write data to SQL
        # need to start from DEVICE becouse other tables refer to it
        try:
            tabs, dat = prepare_tab(
                        dat=new_stock.copy(),
                        tabs=["DEVICE",'BOM'],
                        file=file,
                        row_shift=import_format[args.format]["header"],
                        )
            # existing device for summary reasons
            ex_devs = getL(tab='BOM', get=['device_id'])
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

        # SUMMARY
        msg.BOM_import_summary(new_stock, 
                            len(new_stock[new_stock["device_id"].isin(ex_devs)]))