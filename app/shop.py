from argparse import Namespace
import pandas as pd

from app.common import check_dir_file, read_json
from app.sql import put, getL
from app.tabs import columns_align, prepare_tab
from conf.config import import_format, SQL_scheme
from app.error import messageHandler, prepare_tabError
from pandas.errors import ParserError

msg = messageHandler()


def cart_import(args: Namespace) -> None:
    xls_files = check_dir_file(args)
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

        # rename (and tidy) columns according to format of imported file
        new_stock = columns_align(
            new_stock.copy(),
            file=file,
            supplier=args.format,
        )
        # write data to SQL
        try:
            tabs, dat = prepare_tab(
                            dat=new_stock.copy(),
                            tabs=["DEVICE", "SHOP"],
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
    if imported_files != []:
        ex_devs = new_stock[new_stock["device_id"].isin(getL(tab='BOM', get=['device_id']))]
        msg.BOM_import_summary(imported_files, 
                            new_stock, 
                            len(ex_devs))