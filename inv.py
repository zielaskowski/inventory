import os
import argparse

from app.bom import bom
from app.transaction import trans
from app.commit import commit
from app.sql import sql_check
from app.error import sql_checkError, sql_createError
from conf.config import import_format

if __name__ == "__main__":
    cli = argparse.ArgumentParser(
        description="""
        INVentory management system.
        store information about available stock, devices info, and shop cost.
        Also store projects (list of devices) and assemblies (list of projects).
        
        Scan all BOMs and put into transaction DB (each element into separate row).
        Always write to device table and shop table (also when not adding to stock)
        add to stock when commiting BOM
        Output is written in stock.sqldb file. If the file is not found, it will be created based on sql_scheme.jsonc file
        If found BOM file which is already present in stock.sqldb, it will overwrite this BOM only if commited.
        Comitting the transaction will freze it (not possible to remove) and move all not confirmed transaction to end of tab
        from not commited transaction create common BOM considering the stock
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    command_parser = cli.add_subparsers(title="commands", dest="command")

    cli_bom = command_parser.add_parser(
        "bom", help="scan BOMs and put into transaction DB"
    )
    cli_bom.add_argument(
        "-d",
        "--dir",
        default=os.getcwd(),  # for jupyter: os.path.dirname(os.path.abspath(__file__))
        help="Directory to start scan with. If omitted, current directory is used",
        required=False,
    )
    cli_bom.add_argument(
        "-f",
        "--file",
        help="""
        xls\\xlsx file name to add/replace in stock.csv.
        Will import only files where this FILE is within file name. Case sensitive
        """,
        required=False,
    )
    cli_bom.add_argument(
        "-q",
        "--qty",
        help="multiply 'Order Qty.' by this value. If omitted, no multiplication is done, if q=-1 will ask for value for each BOM",
        required=False,
        default=1,
    )
    cli_bom.add_argument(
        "-F",
        "--format",
        help=f"format of file to import, possible values: {list(import_format.keys())}. Defoult is {list(import_format.keys())[0]}",
        required=False,
        default=list(import_format.keys())[0],  # LCSC
    )
    cli_bom.set_defaults(func=bom)

    cli_transact = command_parser.add_parser(
        "transact",
        allow_abbrev=True,
        help="""Prepare 'shoping list' file from previously imported BOM considering stock.
        Can also prepare file based on project name""",
    )
    cli_transact.add_argument(
        "-f", "--file", help="file name to save shoping list", required=False
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
    cli_transact.set_defaults(func=trans)

    cli_commit = command_parser.add_parser(
        "commit", help="commit BOM table and update stock. Write new project"
    )
    cli_commit.add_argument(
        "-p", "--project", help="project name to commit", required=True
    )
    cli_commit.set_defaults(func=commit)

    args = cli.parse_args()

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
