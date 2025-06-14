"""
utility functions
do not import from other files in project (except error.py & config.py)
as this will cause circular import
"""

import argparse
import json
import os
import re
import shutil
import sys
from json import JSONDecodeError
from string import Template
from typing import Dict

from app.error import (
    ambigous_matchError,
    check_dirError,
    no_matchError,
    read_jsonError,
    scan_dir_permissionError,
    sql_tabError,
    write_jsonError,
)
from app.message import messageHandler
from conf.config import (
    LOG_FILE,
    MAN_ALT,
    SCAN_DIR,
    SQL_SCHEME,
    TEMP_DIR,
    import_format,
    module_path,
)

# list of keywords to be ignored during reading columns from tab
SQL_KEYWORDS = ["FOREIGN", "UNIQUE", "ON_CONFLICT", "HASH_COLS", "COL_DESCRIPTION"]
DEV_ID = "device_id"
DEV_MAN = "device_manufacturer"
DEV_DESC = "device_description"
DEV_PACK = "package"
DEV_HASH = "hash"
BOM_FILE = "import_file"
BOM_DIR = "project_dir"
BOM_COMMITED = "commited"
BOM_PROJECT = "project"
BOM_HASH = "device_hash"
BOM_FORMAT = "file_format"
SHOP_HASH = "device_hash"
SHOP_SHOP = "shop"
SHOP_DATE = "date"
TAKE_LONGER_COLS = [DEV_MAN, DEV_DESC, DEV_PACK]
HIDDEN_COLS = [
    BOM_DIR,
    BOM_FILE,
    BOM_COMMITED,
    BOM_FORMAT,
    "id",
    DEV_HASH,
    BOM_HASH,
    SHOP_DATE,
    SHOP_SHOP,
]  # columns automatically filled, no need to import
NO_EXPORT_COLS = [
    DEV_HASH,
    BOM_HASH,
    SHOP_HASH,
    "id",
]  # columns not exported
IMPORT_FORMAT_SPECIAL_KEYS = ["cols", "dtype", "func", "file_ext"]

msg = messageHandler()

# probably debugpy can be detected in inv.main()
# and set DEBUG apropriately, also used in pytest
# mainly used to skip user interaction parts
DEBUG = False


def store_alternatives(
    alternatives: dict[str, list[str]],
    selection: list[str],
) -> None:
    """
    write selection made by user, so next time no need to choose
    only for DEV_MAN column and only if selection was one-to-one
    """
    if alternatives == {}:
        return
    alt_keys = list(alternatives.keys())
    alt_len = len(alternatives[alt_keys[0]])
    alt_from = alternatives[alt_keys[0]]
    alt_options = alternatives[alt_keys[1]]
    try:
        alt_exist = read_json_list(MAN_ALT)
    except read_jsonError as e:
        msg.msg(str(e))
        return

    for i in range(alt_len):
        # only one-to-one alternatives and changed
        if alt_from[i] != selection[i] and len(alt_options[i].split(" | ")) == 1:
            # remove alternative if already exists
            for k in alt_exist.keys():
                if alt_from[i] in alt_exist[k]:
                    alt_exist[k].remove(alt_from[i])
                    if alt_exist[k] == []:
                        alt_exist.pop(k)
            if selection[i] in alt_exist.keys():
                alt_exist[selection[i]].append(alt_from[i])
                alt_exist[selection[i]] = list(set(alt_exist[selection[i]]))
            else:
                alt_exist[selection[i]] = [alt_from[i]]

    write_json(MAN_ALT, alt_exist)


def get_alternatives(manufacturers: list[str]) -> list[str]:
    """check if we have match from stored alternative"""
    try:
        alt_exist = read_json_list(MAN_ALT)
    except read_jsonError as e:
        msg.msg(str(e))
        return manufacturers
    man_replaced = []
    for man in manufacturers:
        rep = [k for k, v in alt_exist.items() if man in v]
        if rep != []:
            man_replaced.append(rep[0])
        else:
            man_replaced.append(man)
    return man_replaced


def vimdiff_config(panel_name: list[str], column: str):
    """
    prepare vimdiff config from template
    and adjust help message to column
    expeced first key in panel name is:
        /directory/column.txt
    for example: /tmp/manufacturer.txt, /tmp/description.txt, etc.
    """
    column_file = os.path.basename(panel_name[0])
    column = column_file.split(".")[0]
    if "manufacturer" in column:
        with open(
            module_path() + "/vimdiff_help_multiple_manufacturers.txt",
            mode="r",
            encoding="UTF8",
        ) as f:
            manufacturer_help = f.read()
    else:
        manufacturer_help = ""
    with open(
        module_path() + "/vimdiff_help.txt",
        mode="r",
        encoding="UTF8",
    ) as f:
        help_temp = Template(f.read())
    with open(
        module_path() + "/.vimrc",
        mode="r",
        encoding="UTF8",
    ) as f:
        vimrc_temp = Template(f.read())

    substitutions = {
        "TEMP_DIR": TEMP_DIR,
        "LEFT_NAME": panel_name[1],
        "RIGHT_NAME": panel_name[0],
        "COLUMN": column,
        "MULTIPLE_MANUFACTURERS": manufacturer_help,
    }
    vimrc_txt = vimrc_temp.safe_substitute(substitutions)
    help_txt = help_temp.safe_substitute(substitutions)
    with open(TEMP_DIR + ".vimrc", mode="w", encoding="UTF8") as f:
        f.write(vimrc_txt)
    with open(TEMP_DIR + "vimdiff_help.txt", "w", encoding="UTF8") as f:
        f.write(help_txt)


def log(args) -> None:
    """
    log command in ./conf/log.txt
    skip  -h and --help commands
    check if attribute 'help' exists in com namespac
    """
    if LOG_FILE == "":
        return
    if any(a for a in args if a in ["--help", "-h"]):
        return

    cmd = ["python -m inv"] + args

    # check if path exists, if not create
    path = os.path.dirname(LOG_FILE)
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except PermissionError as e:
            msg.log_path_error(str(e))
            return

    try:
        with open(LOG_FILE, "a", encoding="UTF8") as f:
            f.write(f"{' '.join(cmd)}\n")
    except IsADirectoryError as e:
        msg.log_path_error(str(e) + ". missing filename.")
        return


def write_json(file: str, content: dict[str, dict] | dict[str, list[str]]) -> None:
    """
    write content to afile in json format
    overwrite existing file
    """
    try:
        with open(file, mode="w", encoding="UTF8") as f:
            json.dump(content, f)
    except JSONDecodeError as err:
        raise write_jsonError(file) from err


def read_json_dict(file: str) -> Dict[str, dict]:
    """
    read json file where values are dictionary
    ignores comments: everything from '//**' to eol
    raise read_jsonError if format is wrong or file do not exists
    """
    try:
        with open(file, "r", encoding="UTF8") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()), flags=re.MULTILINE
            )
    except FileNotFoundError as err:
        raise read_jsonError(file) from err

    try:
        data = json.loads(json_f)
        if not all(isinstance(v, dict) for v in data.values()):
            raise read_jsonError(file, type_val="dictionary")
    except JSONDecodeError as err:
        raise read_jsonError(file) from err
    return data


def read_json_list(file: str) -> Dict[str, list[str]]:
    """
    read json file where values are list
    ignores comments: everything from '//**' to eol
    raise read_jsonError if format is wrong or file do not exists
    """
    try:
        with open(file, "r", encoding="UTF8") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()), flags=re.MULTILINE
            )
    except FileNotFoundError as err:
        raise read_jsonError(file) from err

    try:
        data = json.loads(json_f)
        if not all(isinstance(v, list) for v in data.values()):
            raise read_jsonError(file, type_val="list")
    except JSONDecodeError as err:
        raise read_jsonError(file) from err
    return data


def find_files(directory: str, file_format: str) -> list:
    """
    scan all subdirectories (starting from 'directory')
    search for any xlsx or xls file, only in BOM folder
    in case format=='csv', search for *.csv files
    return a list of full path+file_name
    """
    if not os.access(directory, os.W_OK):
        raise scan_dir_permissionError(directory=directory)
    console_width = shutil.get_terminal_size().columns
    match_files = []
    file_ext = import_format[file_format]["file_ext"]
    s_dir = SCAN_DIR
    msg.msg(f"searching for *.{file_ext} files in {s_dir} folder:")
    for folder, _, files in os.walk(directory):
        for file in files:
            txt = "Searching... " + folder
            print(txt[-console_width:], end="\r")
            # drop last '/' in directory with regex
            # change default behaviour but nobody understand these niuanses
            cur_dir = re.sub("/$", "", folder)
            cur_dir = cur_dir.split("/")[-1].upper()
            if s_dir == "":
                s_dir = cur_dir
            if cur_dir == s_dir.upper():
                if any(file.endswith(e) for e in file_ext):
                    match_files.append(os.path.join(folder, file))
    print(" " * 200, end="\r")
    return match_files


def check_dir_file(args: argparse.Namespace) -> list[str]:
    """
    check search directory if exists
    and check if file exists
    return found files
    """
    if not os.path.exists(args.dir):
        raise check_dirError(file=args.file, directory=args.dir, scan_dir=SCAN_DIR)
    files = find_files(args.dir, args.format)

    # filter by file name
    if args.file is not None:
        files = [f for f in files if args.file in os.path.basename(f)]
        if files == []:
            raise check_dirError(file=args.file, directory=args.dir, scan_dir=SCAN_DIR)
    return files


def unpack_foreign(foreign: dict[str, str]) -> tuple[str, str, str]:
    """
    read foreign key from sql_scheme
    returns col which is connected and destination table and column
    """
    col = list(foreign.keys())[0]
    # get foreign table and column
    f_tab_col = list(foreign.values())[0]

    # get foreign_tab
    (
        f_tab,
        f_col,
    ) = f_tab_col.split("(")
    f_col = f_col.replace(")", "")
    return col, f_tab, f_col


def tab_exists(tab: str) -> None:
    """
    check if tab exists
    raises sql_tabError if not
    """
    sql_scheme = read_json_dict(SQL_SCHEME)
    if tab not in sql_scheme.keys():
        raise sql_tabError(tab, sql_scheme.keys())


def tab_cols(
    tab: str,
) -> tuple[list[str], list[str]]:
    """
    return list of columns that are required for the given tab
    and list of columns that are "nice to have"
    follow FOREIGN key constraints to other tab
    """
    sql_scheme = read_json_dict(SQL_SCHEME)
    tab_exists(tab)  # will raise sql_tabError if not

    cols = list(sql_scheme.get(tab, ""))
    must_cols = [
        c
        for c in cols
        if any(cc in sql_scheme[tab][c] for cc in ["NOT NULL", "PRIMARY KEY"])
    ]
    nice_cols = [c for c in cols if "NOT NULL" not in sql_scheme[tab][c]]
    nice_cols = [c for c in nice_cols if "PRIMARY KEY" not in sql_scheme[tab][c]]

    if "UNIQUE" in cols:
        for u in sql_scheme[tab]["UNIQUE"]:
            must_cols += [u]
            if u in nice_cols:
                nice_cols.remove(u)

    if "FOREIGN" in cols:
        for f in sql_scheme[tab]["FOREIGN"]:
            col, foreign_tab, _ = unpack_foreign(f)

            nice_cols = [c for c in nice_cols if c not in col]
            must_cols = [c for c in must_cols if c not in col]

            # get foreign columns
            (
                foreign_must,
                foreign_nice,
            ) = tab_cols(foreign_tab)
            must_cols += foreign_must
            nice_cols += foreign_nice

    # remove duplicates
    must_cols = list(set(must_cols))
    nice_cols = list(set(nice_cols))

    # remove COMMANDS and ['id', 'hash] column
    nice_cols = [c for c in nice_cols if c not in SQL_KEYWORDS + HIDDEN_COLS]
    must_cols = [c for c in must_cols if c not in HIDDEN_COLS]
    return (must_cols, nice_cols)


def foreign_tabs(tab: str) -> list[str]:
    """return list of tables refrenced in FOREIGN key"""
    tab_exists(tab)  # will raise sql_tabError if not
    sql_scheme = read_json_dict(SQL_SCHEME)
    tabs = []
    f_keys = sql_scheme[tab].get("FOREIGN", [])
    for k in f_keys:
        _, f_tab, _ = unpack_foreign(k)
        tabs += [f_tab]
    return tabs


def print_file(file: str):
    """print file"""
    try:
        with open(file, "r", encoding="UTF8") as f:
            print(f.read())
    except FileNotFoundError:
        print(f"File {file} not found")


def match_from_list(cmd: str, choices: Dict | list) -> str:
    """
    try match cmd in list. Return full cmd if only one match.
    Other way return raises no_matchError or ambiguos_matchError
    """
    matches = [choice for choice in choices if choice.startswith(cmd)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ambigous_matchError(cmd=cmd, matches=matches)
    raise no_matchError(cmd=cmd)


class AbbreviationParser(argparse.ArgumentParser):
    """override argparser to provide arguments abbrevation"""

    def _get_abbreviation(self, cmd: str, choices: Dict) -> str:
        if cmd in ["-h", "--help"]:
            return cmd
        try:
            return match_from_list(cmd=cmd, choices=choices)
        except ambigous_matchError as err:
            self.error(str(err))
        except no_matchError as err:
            self.error(str(err))

    def parse_args(self, args: list | None = None, namespace=None):  # type: ignore
        if args is None:
            args = sys.argv[1:]
        # only subcommand given so show help
        if len(args) == 1:
            args += ["-h"]

        # in case choices are None
        try:
            choices: Dict = self._subparsers._actions[1].choices
        except AttributeError:
            self.error("No arguments added to argparser")

        # Check if the first positional argument is an abbreviation of a subcommand
        if args and args[0] in choices:
            return super().parse_args(args, namespace)

        if args and args[0]:
            full_cmd = self._get_abbreviation(args[0], choices)
            args[0] = full_cmd  # pyright: ignore
        return super().parse_args(args, namespace)
