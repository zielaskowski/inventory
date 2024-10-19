import os
import sys
import argparse

from app.bom import bom_import
from app.shop import cart_import
from app.transaction import trans
from app.commit import commit
from app.admin import admin
from app.sql import sql_check
from app.common import log, AbbreviationParser
from app.error import sql_checkError, sql_createError
from conf.config import import_format

if __name__ == "__main__":
    cli = AbbreviationParser(
        description="""
        INVentory management system.
        store information about available stock, devices info, and shop cost.
        Also store projects (list of devices).
        All imported data are temporary stored in BOM table, allowing creation
        of common BOM and shop list. When comited, all ements from BOM table are
        transfered to stock table and project is created.
        
        Scan all BOMs and put into BOM table (each element into separate row).
        Scan only files inside 'bom|BOM' folder. Can be changed in config.py.
        Always write to device table and shop table (also when not adding to stock)
        add to stock when commiting BOM
        Output is written in stock.sqldb file. If the file is not found, it will be created based on sql_scheme.jsonc file
        If found BOM file which is already present in stock.sqldb, it will overwrite this BOM only if commited.
        Comitting the transaction will freze it (not possible to remove) and move all not confirmed transaction to end of tab
        from not commited transaction create common BOM considering the stock
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    command_parser = cli.add_subparsers(
        title="commands", 
        dest="command")

    cli_import_bom = command_parser.add_parser(
        "bom_import",
        allow_abbrev=True,
        help="Scan xls/csv files and add into BOM table.",
    )
    cli_import_bom.add_argument(
        "-d",
        "--dir",
        default=os.getcwd(),  # for jupyter: os.path.dirname(os.path.abspath(__file__))
        help="""Directory to start scan with. 
                If omitted, current directory is used. 
                Scan only in 'BOM' folder (can be change in config.py)""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-f",
        "--file",
        help="""xls/xlsx file to import. Select proper format (can be extendeed in config.py).
                Will import only files where FILE is within file name. Case sensitive.""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-F",
        "--format",
        help=f"format of file to import, possible values: {list(import_format.keys())}. Defoult is {list(import_format.keys())[0]}",
        required=False,
        default=list(import_format.keys())[0],  # LCSC
    )
    cli_import_bom.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="""During import: replace items already imported from the same file.
                If omitted, items will be added to existing items from the same file.""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-i",
        "--reimport",
        action="store_true",
        help="""Import again (and replace) items in BOM table.""",
    )
    cli_import_bom.add_argument(
        "-r",
        "--remove",
        action="store_true",
        help="""Remove fromBOM table: remove all items. 
                Filter with --file if given
                Do not touch any ather table in DB.""",
    )
    cli_import_bom.set_defaults(func=bom_import)

    cli_import_cart = command_parser.add_parser(
        "cart_import",
        allow_abbrev=True,
        help="Scan for xls files and import shopping chart",
    )
    cli_import_cart.add_argument(
        "-d",
        "--dir",
        default=os.getcwd(),  # for jupyter: os.path.dirname(os.path.abspath(__file__))
        help="""Directory to start scan with. 
                If omitted, current directory is used. 
                Scan only in 'BOM' folder (can be change in config.py)""",
        required=False,
    )
    cli_import_cart.add_argument(
        "-f",
        "--file",
        help="""xls/xlsx file to import. Select proper format (can be extendeed in config.py).
                Will import only files where this FILE is within file name. Case sensitive.""",
        required=False,
    )
    cli_import_cart.add_argument(
        "-F",
        "--format",
        help=f"format of file to import, possible values: {list(import_format.keys())}. Defoult is {list(import_format.keys())[0]}",
        required=False,
        default=list(import_format.keys())[0],  # LCSC
    )
    cli_import_cart.set_defaults(func=cart_import)

    cli_transact = command_parser.add_parser(
        "transact",
        allow_abbrev=True,
        help="""Prepare 'shopping list' file from previously imported BOM considering stock.
        Can also prepare file based on project name""",
    )
    cli_transact.add_argument(
        "-f",
        "--file",
        help="File name to save shoping list, defoult is 'shopping list.csv'",
        required=False,
        default="shopping_list.csv",
    )
    cli_transact.add_argument(
        "-d",
        "--dir",
        default=os.getcwd(),  # for jupyter: os.path.dirname(os.path.abspath(__file__))
        help="Directory to save shoping list. If omitted, current directory is used",
        required=False,
    )
    cli_transact.add_argument(
        "-p",
        "--project",
        help="project name used to prepare shoping list",
        required=False,
    )
    cli_transact.add_argument(
        "-q",
        "--qty",
        help="multiply 'Order Qty.' by this value. If omitted, no multiplication is done, if q=-1 will ask for value for each BOM",
        required=False,
        default=1,
    )
    cli_transact.set_defaults(func=trans)

    cli_commit = command_parser.add_parser(
        "commit", 
        help="commit BOM table and update stock. Write new project"
    )
    cli_commit.add_argument(
        "-p", "--project", help="project name to commit", required=True
    )
    cli_commit.set_defaults(func=commit)

    cli_admin = command_parser.add_parser(
        "admin", help="Admin functions. Be responsible."
    )
    admin_group = cli_admin.add_mutually_exclusive_group(required=True)
    admin_group.add_argument(
        "-c",
        "--config",
        action="store_true",
        help="Show current config",
    )
    admin_group.add_argument(
        "--remove_dev_id",
        nargs="+",
        default=False,
        help="""Clean all devices from list using device part number. Also from all other tables.
                Refuse to remove devices used in project; use force to overcome.""",
    )
    cli_admin.add_argument(
        "-F",
        "--force",
        action="store_true",
        help="Force remove all devices from list. Including project tables.",
    )
    cli_admin.set_defaults(func=admin)

    args = cli.parse_args()
    log(sys.argv[1:])

    # check if we have proper sql file
    try:
        sql_check()
    except sql_checkError as e:
        print(e)
        exit(1)
    except sql_createError as e:
        print(e)
        exit(1)

    if "func" in args:
        args.func(args)
    else:
        cli.print_help()
