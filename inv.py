#!/home/mi/docs/prog/python/inventory/.venv/bin/python
"""INVentory management system"""
import argparse
import inspect
import os
import sys

from app.admin import admin
from app.common import AbbreviationParser, log, write_json
from app.error import SqlCheckError, SqlCreateError
from app.import_dat import bom_import, shop_import, stock_import
from app.message import MessageHandler
from app.sql import sql_check
from app.transaction import trans
from conf import config as conf

msg = MessageHandler()


def _add_bom_import_parser(command_parser):
    """Adds arguments for the 'bom_import' command."""
    cli_import_bom = command_parser.add_parser(
        "bom_import",
        allow_abbrev=True,
        help="Scan xls/csv files and add into BOM table.",
    )
    cli_import_bom.add_argument(
        "-d",
        "--dir",
        default=".",
        help="""Directory to start scaning for files to be imported.
                Scan only in 'BOM' folder (can be change in config.py)""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-f",
        "--file",
        help="""xls/xlsx file to import. Select proper format (can be extendeed in config.py).
                Will import only files where FILE is within file name. Case sensitive.
                Default: all files.""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-F",
        "--format",
        help=f"format of file to import, possible values: {list(conf.import_format.keys())}.\
                Default is {list(conf.import_format.keys())[1]}",
        required=False,
        default=list(conf.import_format.keys())[1],  # easyEDA
    )
    cli_import_bom.add_argument(
        "-e",
        "--export",
        metavar="PROJECT",
        nargs="+",
        default=None,
        help=f"""Print data from BOM table, you can use abbreviations.
                By default show {conf.BOM_EXPORT_COL} columns.
                See --show_columns to change.
                If --file is given, write to file as csv in --dir folder.
                Use '%%' if you want to export all projects. Use '?'
                (including single quote!) to list available projects.""",
        required=False,
    )
    cli_import_bom.add_argument(
        "--export_columns",
        metavar="COL",
        required=False,
        nargs="+",
        type=str,
        help="""Show columns during export (overwrite default columns).
                See --import for columns description.""",
        default=None,
    )
    cli_import_bom.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="""During import: replace existing project (whole project!).
                If omitted, devices will be added to existing project.""",
        required=False,
    )
    cli_import_bom.add_argument(
        "-r",
        "--reimport",
        action="store_true",
        help="""Import again (and replace, see --overwrite) items in BOM table.
              Replacement is based on file name""",
    )
    cli_import_bom.add_argument(
        "--dont_align_columns",
        action="store_true",
        default=False,
        help="""During import, by default user will be asked if different
                values are imported for other columns (description, category,
                package, etc.) on existing devices. If this option given,
                existing values will be kept (importing data will not change existing devices).""",
        required=False,
    )
    cli_import_bom.add_argument(
        "--info",
        help="""Display info about necessery and acceptable columns for BOM table.""",
        required=False,
        action="store_true",
    )
    cli_import_bom.add_argument(
        "--csv_template",
        help="""Save template csv with all columns to a file.
                Default is './template.csv'""",
        required=False,
        nargs="?",
        const="./template.csv",
        default=None,
    )
    cli_import_bom.set_defaults(func=bom_import)


def _add_shop_cart_import_parser(command_parser):
    """Adds arguments for the 'shop_cart_import' command."""
    cli_import_cart = command_parser.add_parser(
        "shop_cart_import",
        allow_abbrev=True,
        help="Scan for xls files and import shopping cart",
    )
    cli_import_cart.add_argument(
        "-d",
        "--dir",
        default=".",
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
        help=f"format of file to import, possible values: {list(conf.import_format.keys())}.\
                Default is {list(conf.import_format.keys())[0]}",
        required=False,
        default=list(conf.import_format.keys())[0],  # LCSC
    )
    cli_import_cart.add_argument(
        "-e",
        "--export",
        action="store_true",
        default=None,
        help="""Print data from STOCK table, you can use abbreviations.
                If --file is given, write to file as csv in --dir folder.""",
        required=False,
    )
    cli_import_cart.add_argument(
        "--hide_columns",
        metavar="COL",
        required=False,
        nargs="+",
        type=str,
        help="hide columns during export",
        default=None,
    )
    cli_import_cart.add_argument(
        "--dont_align_columns",
        action="store_true",
        default=False,
        help="""During import, by default user will be asked if different
                values are imported for other columns (description, category,
                package, etc.) on existing devices. If this option given,
                existing values will be kept (importing data will not change existing devices).""",
        required=False,
    )
    cli_import_cart.add_argument(
        "--info",
        help="""Display info about necessery and acceptable columns for SHOP table.""",
        required=False,
        action="store_true",
    )
    cli_import_cart.add_argument(
        "--csv_template",
        help="Save template csv with all columns to a file. Default is './template.csv'",
        required=False,
        nargs="?",
        const="./template.csv",
        default=None,
    )
    cli_import_cart.set_defaults(func=shop_import)


def _add_transact_parser(command_parser):
    """Adds arguments for the 'transact' command."""
    cli_transact = command_parser.add_parser(
        "transact",
        allow_abbrev=True,
        help="""Prepare 'shopping list' file for selected projects considering stock,
        if available.""",
    )
    cli_transact.add_argument(
        "-f",
        "--file",
        help="File name to save shoping list (with extension), default is 'shopping_list'",
        required=False,
        default="shopping_list",
    )
    cli_transact.add_argument(
        "-d",
        "--dir",
        default=".",
        help="Directory to save shoping list. If omitted, current directory is used",
        required=False,
    )
    cli_transact.add_argument(
        "-p",
        "--project",
        nargs="+",
        default=["%"],
        help="""Export data from selected PROJECTS, you can use abbreviations.
                Use '%%' if you want to export all projects.
                Use '?' to list available projects. Default is '%%' """,
    )
    cli_transact.add_argument(
        "-q",
        "--qty",
        type=int,
        help="""Multiply 'Order Qty.' by this value. If omitted, no multiplication is done,
                if q=-1 will ask for value for each BOM""",
        required=False,
        default=1,
    )
    cli_transact.add_argument(
        "-s",
        "--dont_split_shop",
        action="store_true",
        default=False,
        help="""Do not split shopping list by supplier, also ignore shop minimum quantity!.
                Split by default.""",
    )
    cli_transact.set_defaults(func=trans)


def _add_stock_parser(command_parser):
    """Adds arguments for the 'stock' command."""
    cli_stock = command_parser.add_parser(
        "stock",
        help="""stock operations: add, remove, display.""",
    )
    cli_stock.add_argument(
        "-d",
        "--dir",
        default=".",
        help="""Directory to start scaning for files to be imported.
                Scan only in 'BOM' folder (can be change in config.py)""",
        required=False,
    )
    cli_stock.add_argument(
        "-f",
        "--file",
        help="""xls/xlsx/csv file to import. Select proper format (can be extendeed in config.py).
                Will import only files where FILE is within file name. Case sensitive.
                Default: all files.""",
        required=False,
    )
    cli_stock.add_argument(
        "-F",
        "--format",
        help=f"format of file to import, possible values: {list(conf.import_format.keys())}.\
                Default is {list(conf.import_format.keys())[4]}",
        required=False,
        default=list(conf.import_format.keys())[4],  # csv_LCSC
    )
    cli_stock.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="""During import: replace existing project (whole project!).
                If omitted, devices will be added to existing project.""",
        required=False,
    )
    cli_stock.add_argument(
        "--dont_align_columns",
        action="store_true",
        help="""During import, by default user will be asked if different
                values are imported for other columns (description, category,
                package, etc.) on existing devices. If this option given,
                existing values will be kept (importing data will not change existing devices).""",
        required=False,
    )
    cli_stock.add_argument(
        "-e",
        "--export",
        action="store_true",
        help=f"""Print data from STOCK table. By default {conf.STOCK_EXPORT_COL}
                columns are displayed. Use --export_columns to change.
                If --file is given, write to file as csv in --dir folder.""",
        required=False,
    )
    cli_stock.add_argument(
        "--export_columns",
        metavar="COL",
        required=False,
        nargs="+",
        type=str,
        help="""Show columns during export (overwrite default columns).
                See --info for column description.""",
    )
    cli_stock.add_argument(
        "--add_project",
        metavar="PROJECT",
        nargs="+",
        help="""Commit PROJECTs. Add all devices from selected projects
                to the stock, use --qty option to multiply quantity.
                Use '%%' if you want to commit all projects.
                Use '?' (including single quote!) to list available projects.""",
    )
    cli_stock.add_argument(
        "--use_project",
        metavar="PROJECT",
        nargs="+",
        help="""Remove PROJECTs devices from stock, use --qty option to multiply quantity.
                Use '%%' if you want to remove all projects.
                Use '?' (including single quote!) to list available projects.""",
    )
    cli_stock.add_argument(
        "-q",
        "--qty",
        type=int,
        default=1,
        required=False,
        help="""Quantity used to multiply device qty inside projects when adding
                or removing (using). Default equal to 1.""",
    )
    cli_stock.add_argument(
        "--add_device_id",
        metavar="DEV_ID",
        help="""Add single DEVICE by its ID, usually connects with --add_device_manufacturer.
                See README.md to see how to use with fuzzy search.""",
    )
    cli_stock.add_argument(
        "--add_device_manufacturer",
        metavar="DEV_MAN",
        help="""Add single DEVICE by its MAUNUFACTURER, usually connects with --add_device_id.
                See README.md to see how to use with fuzzy search.""",
    )
    cli_stock.add_argument(
        "--use_device_id",
        metavar="DEV_ID",
        help="""Remove single DEVICE by its ID, usually connects with --use_device_manufacturer.
                See README.md to see how to use with fuzzy search.""",
    )
    cli_stock.add_argument(
        "--use_device_manufacturer",
        metavar="DEV_MAN",
        help="""Remove single DEVICE by its MAUNUFACTURER, usually connects with --use_device_id.
                See README.md to see how to use with fuzzy search.""",
    )
    cli_stock.add_argument(
        "--info",
        help="""Display info about necessery and acceptable columns for STOCK table.""",
        action="store_true",
    )
    cli_stock.add_argument(
        "--csv_template",
        help="Save template csv with all columns to a file. Default is './template.csv'",
        nargs="?",
        const="./template.csv",
    )
    cli_stock.add_argument(
        "--fzf",
        help=f"""
        Used by script as input for fuzzy finder. Rather use script itself: {conf.MODULE_PATH}/conf/inv_fzf.sh then this option.
        """,
        action="store_true",
    )
    cli_stock.add_argument(
        "--history",
        help="""Display history about operations on stock table. Can be used to undo.""",
        action="store_true",
    )
    cli_stock.add_argument(
        "--undo",
        help="""History id to be undone (see --history). Can be single line id
                or range in format 5..15. When digit missing range from begining
                or end (begining is on left side): ..5 or 15..""",
    )
    cli_stock.set_defaults(func=stock_import)


def _add_admin_parser(command_parser):
    """Adds arguments for the 'admin' command."""
    cli_admin = command_parser.add_parser(
        "admin", help="Admin functions. Be responsible."
    )
    admin_group = cli_admin.add_mutually_exclusive_group(required=True)
    admin_group.add_argument(
        "-c",
        "--display_config",
        action="store_true",
        help="Show current config",
    )
    admin_group.add_argument(
        "--set_local_config",
        action="store_true",
        help="Set config in local directory. You can then adjust manualy.",
    )
    admin_group.add_argument(
        "-a",
        "--align_manufacturers",
        action="store_true",
        help="""align manufacturers for the same dvices. Will ask user for selection""",
    )
    admin_group.add_argument(
        "--remove_project",
        metavar="PROJECT",
        nargs="+",
        help="""Remove from BOM table all items from PROJECTs.
                Use '%%' if you want to remove all projects.
                Use '?' (including single quote!) to list available projects.
                Do not touch any other table in DB.""",
    )
    admin_group.add_argument(
        "--remove_dev_id",
        metavar="DEV_ID",
        nargs="+",
        default=False,
        help="""Clean all devices from list using device part number. Also from
                all other tables. You can read part number from csv file,
                see --csv option. Refuse to remove devices used in project;
                use force to overcome.""",
    )
    admin_group.add_argument(
        "--remove_shop_id",
        metavar="SHOP_ID",
        nargs="+",
        default=False,
        help="""Remove shop part number from shop table.
                You can read part number from csv file, see --csv option.
                Usefull when item not in shop stock any more""",
    )
    admin_group.add_argument(
        "--force",
        action="store_true",
        help="Force device removal also when used in project",
    )
    cli_admin.add_argument(
        "--csv",
        help="""read device_id list from csv file. See --what_col and
                --filter_col, --filter_val for column selection and filtering""",
    )
    cli_admin.add_argument(
        "-w",
        "--what_col",
        help="Which column to read from csv file.",
    )
    cli_admin.add_argument(
        "-f",
        "--filter_col",
        help="Which column to use for filtering in csv file.",
    )
    cli_admin.add_argument(
        "-v",
        "--filter_val",
        help="What value use to filter in filter_col in csv file.",
    )
    cli_admin.set_defaults(func=admin)


def cli_parser() -> AbbreviationParser:
    """command line parser definition"""
    cli = AbbreviationParser(
        description=f"""
        INVentory management system.
        Store information about available stock, devices info, and shop cost.
        Also store BOM projects (list of devices).
        
        Typical workflow can be:
        Scan all BOM files from location (including subfolders) and put 
        into BOM table (each element into separate row). Scan only files 
        inside 'bom|BOM' folder. Can be changed in config.py. Each imported file 
        will be treated as project. You can combine multiple project and export to
        file suitable for importing into shop cart. There is also a function to 
        import Shoping cart with prices. If you import shop cart from many shops,
        BOM can export separate file for each shop considering best cost combination.
        Finaly, you can commit the selected projects, which will store the
        devices in the STOCK table.

        Output is written in stock.sql db file. If the file is not found, it will be
        created based on sql_scheme.jsonc file.
        Application can import exccel files in different formats (from different)
        shops. Format description is in config file. Should be easy to extend.
        Each execution of app is writing used arguments into log file. You cen setup 
        in {conf.CONFIG_PATH}.config.py file.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    command_parser = cli.add_subparsers(title="commands", dest="command")

    _add_bom_import_parser(command_parser)
    _add_shop_cart_import_parser(command_parser)
    _add_transact_parser(command_parser)
    _add_stock_parser(command_parser)
    _add_admin_parser(command_parser)

    return cli


if __name__ == "__main__":
    # when dubuging with debugpy, it should be somwhere in path
    # of one of the stack frame
    try:
        if "debugpy" in inspect.stack()[1].filename:
            conf.DEBUG = "debugpy"  # pyright: ignore
    except IndexError:
        # normall call (no debug)
        pass
    # check if file with manufacturer alternatives exists
    if not os.path.exists(conf.MAN_ALT):
        write_json(conf.MAN_ALT, {"": [""]})
    parser = cli_parser()
    args = parser.parse_args()

    # check if we have proper sql file
    try:
        sql_check()
    except SqlCheckError as e:
        msg.msg(str(e))
        sys.exit(1)
    except SqlCreateError as e:
        msg.msg(str(e))
        sys.exit(1)

    if "func" in args:
        args.func(args)
        log(args)
    else:
        parser.print_help()
