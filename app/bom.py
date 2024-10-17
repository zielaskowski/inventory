import os
import pandas as pd
from pandas.errors import ParserError
from argparse import Namespace

from app.sql import getL, getDF, rm
from app.common import check_dir_file
from app.tabs import __columns_align__, write_tab
from conf.config import import_format
from app.error import messageHandler, write_bomError, check_dirError

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
        new_stock = __columns_align__(
            new_stock.copy(),
            file=file,
            supplier=args.format,
        )

        # write data to SQL
        # need to start from DEVICE becouse other tables refer to it
        try:
            write_tab(
                dat=new_stock.copy(),
                tab="DEVICE",
                qty=args.qty,
                file=file,
                row_shift=import_format[args.format]["header"],
            )
            write_tab(
                dat=new_stock.copy(),
                tab="BOM",
                qty=args.qty,
                file=file,
                row_shift=import_format[args.format]["header"],
            )
            write_tab(
                dat=new_stock.copy(),
                tab="SHOP",
                qty=args.qty,
                file=file,
                row_shift=import_format[args.format]["header"],
            )
        except write_bomError as e:
            print(e)
            continue
        imported_files.append(file)

    # summary
    msg.BOM_import_summary(imported_files, new_stock)
