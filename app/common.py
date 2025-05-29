"""utility functions"""

import argparse
import json
import os
import re
import sys
from json import JSONDecodeError
from typing import Dict

from app.error import check_dirError, messageHandler, read_jsonError
from conf.config import LOG_FILE, SCAN_DIR, SQL_KEYWORDS, SQL_SCHEME

msg = messageHandler()


def log(args) -> None:
    """
    log command in ./conf/log.txt
    skip  -h and --help commands
    check if attribute 'help' exists in com namespac
    """
    if any(a for a in args if a in ["--help", "-h"]):
        return

    cmd = ["python -m inv"] + args

    # check if path exists, if not create
    path = os.path.dirname(LOG_FILE)
    if not os.path.exists(path):
        os.makedirs(path)

    with open(LOG_FILE, "a", encoding="UTF8") as f:
        f.write(f"{' '.join(cmd)}\n")


def read_json(file: str) -> Dict[str, dict]:
    """read json file
    ignores comments: everything from '//**' to eol"""
    try:
        with open(file, "r", encoding="UTF8") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()), flags=re.MULTILINE
            )
    except FileNotFoundError as err:
        raise read_jsonError(file) from err

    try:
        return json.loads(json_f)
    except JSONDecodeError as err:
        raise read_jsonError(file) from err


def find_xlsx_files(directory: str) -> list:
    """
    scan all subdirectories (starting from 'directory')
    search for any xlsx or xls file, only in BOM folder
    return a list of full path+file_name
    """
    xlsx_files = []
    s_dir = SCAN_DIR
    for folder, _, files in os.walk(directory):
        for file in files:
            # drop last '/' in directory with regex
            # change defoult behaviour but nobody understand these niuanses
            cur_dir = re.sub("/$", "", folder)
            cur_dir = cur_dir.split("/")[-1].upper()
            if s_dir == "":
                s_dir = cur_dir
            if cur_dir == s_dir.upper():
                if file.endswith(".xlsx") or file.endswith(".xls"):
                    xlsx_files.append(os.path.join(folder, file))
    return xlsx_files


def check_dir_file(args: argparse.Namespace) -> list[str]:
    """
    check search directory if exists
    and check if file exists
    return found files
    """
    if not os.path.exists(args.dir):
        raise check_dirError(file=args.file, directory=args.dir, scan_dir=SCAN_DIR)
    xlsx_files = find_xlsx_files(args.dir)

    # filter by file name
    if args.file is not None:
        xlsx_files = [f for f in xlsx_files if args.file in f]
        if xlsx_files == []:
            raise check_dirError(file=args.file, directory=args.dir, scan_dir=SCAN_DIR)
    return xlsx_files


def unpack_foreign(foreign: dict[str, str]) -> tuple[str, str, str]:
    """
    read foreign key from sqk_scheme
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


def tab_cols(
    tab: str,
) -> tuple[list[str], list[str]]:
    """
    return list of columns that are required for the given tab
    and list of columns that are "nice to have"
    follow FOREIGN key constraints to other tab
    """
    sql_scheme = read_json(SQL_SCHEME)
    if tab not in sql_scheme:
        raise ValueError(f"Table {tab} does not exists in SQL_scheme")

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
    nice_cols = [c for c in nice_cols if c not in SQL_KEYWORDS + ["id", "hash"]]
    must_cols = [c for c in must_cols if c not in ["id", "hash"]]
    return (must_cols, nice_cols)


def print_file(file: str):
    """print file"""
    try:
        with open(file, "r", encoding="UTF8") as f:
            print(f.read())
    except FileNotFoundError:
        print(f"File {file} not found")


class AbbreviationParser(argparse.ArgumentParser):
    """override argparser to provide arguments abbrevation"""

    def _get_abbreviation(self, cmd: str, choices: Dict) -> str:
        if cmd in ["-h", "--help"]:
            return cmd
        matches = [choice for choice in choices if choice.startswith(cmd)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            self.error(f"Ambiguous abbreviation '{cmd}', match: {matches}.")
        else:
            self.error(f"No match found for abbreviation '{cmd}'.")

    def parse_args(self, args=None, namespace=None):  # pyright: ignore
        if args is None:
            args = sys.argv[1:]
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
