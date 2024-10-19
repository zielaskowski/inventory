import json
import hashlib
from typing import Dict
import argparse
import re
import os
import sys
import pandas as pd

from json import JSONDecodeError

from conf.config import SQL_scheme, SQL_keywords, scan_dir, log_file
from app.error import read_jsonError, check_dirError, hashError
from app.error import messageHandler


msg = messageHandler()


def log(args) -> None:
    # log command in ./conf/log.txt
    # skip  -h and --help commands
    #check if attribute 'help' exists in com namespac
    if any(a for a in args if a in ["--help", "-h"]):
        return
    
    cmd = ['python -m inv'] + args
    
    # check if path exists, if not create
    path = os.path.dirname(log_file)
    if not os.path.exists(path):
        os.makedirs(path)
    
    with open(log_file, "a") as f:
        f.write(f"{' '.join(cmd)}\n")


def read_json(file: str) -> Dict:
    """read json file
    ignores comments: everything from '//**' to eol"""
    try:
        with open(file, "r") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()), flags=re.MULTILINE
            )
    except IOError:
        raise read_jsonError(file)

    try:
        return json.loads(json_f)
    except JSONDecodeError:
        raise read_jsonError(file)


def find_xlsx_files(directory: str) -> list:
    # scan all subdirectories (starting from 'directory')
    # search for any xlsx or xls file, only in BOM folder
    # return a list of full path+file_name
    xlsx_files = []
    s_dir = scan_dir
    for dir, _, files in os.walk(directory):
        for file in files:
            # drop last '/' in directory with regex
            # change defoult behaviour but nobody understand these niuanses
            cur_dir = re.sub("/$", "", dir)
            cur_dir = cur_dir.split("/")[-1].upper()
            if s_dir == "":
                s_dir = cur_dir
            if cur_dir == s_dir.upper():
                if file.endswith(".xlsx") or file.endswith(".xls"):
                    xlsx_files.append(os.path.join(dir, file))
    return xlsx_files


def check_dir_file(args: argparse.Namespace) -> list[str]:
    # check search directory
    if not os.path.exists(args.dir):
        raise check_dirError(args.file, args.dir, scan_dir)
    xlsx_files = find_xlsx_files(args.dir)

    # filter by file name
    if args.file is not None:
        xlsx_files = [f for f in xlsx_files if args.file in f]
        if xlsx_files == []:
            raise check_dirError(args.file, args.dir, scan_dir)
    return xlsx_files


def hash_table(dat: pd.Series, cols: list[str]) -> pd.Series:
    if not all(c in dat for c in cols):
        raise hashError(cols)

    # d = dat.copy(deep=True)
    return hashlib.md5("".join(list(dat[cols])).encode("utf-8")).hexdigest()


def unpack_foreign(foreign: list[dict[str, str]]) -> tuple[str]:
    # read foreign key from sqk_scheme
    # returns col which is connected and destination table and column
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
) -> list[list[str], list[str], dict[str : list[str]],]:
    # return list of columns that are required for the given tab
    # and list of columns that are "nice to have"
    # follow FOREIGN key constraints to other tab
    # and check HASH_COLS if exists in other tab
    # hashed col is not in must_cols, as it will be created
    sql_scheme = read_json(SQL_scheme)
    if tab not in sql_scheme:
        raise ValueError(f"Table {tab} does not exists in SQL_scheme")

    cols = list(sql_scheme.get(tab))
    must_cols = [
        c
        for c in cols
        if any(cc in sql_scheme[tab][c] for cc in ["NOT NULL", "PRIMARY KEY"])
    ]
    # must_cols = [c for c in must_cols if "PRIMARY KEY" not in sql_scheme[tab][c]]
    nice_cols = [c for c in cols if "NOT NULL" not in sql_scheme[tab][c]]
    nice_cols = [
        c for c in nice_cols if "PRIMARY KEY" not in sql_scheme[tab][c]
    ]
    hash_dic = {}

    if "HASH_COLS" in cols:
        hash_dic = sql_scheme[tab]["HASH_COLS"]
        hashed_col = list(hash_dic.keys())
        must_cols = [c for c in must_cols if c not in hashed_col]
        nice_cols = [c for c in nice_cols if c not in hashed_col]

    if "UNIQUE" in cols:
        for U in sql_scheme[tab]["UNIQUE"]:
            must_cols += [U]
            if U in nice_cols:
                nice_cols.remove(U)

    if "FOREIGN" in cols:
        for F in sql_scheme[tab]["FOREIGN"]:
            col, foreign_tab, foreign_col = unpack_foreign(F)

            nice_cols = [c for c in nice_cols if c not in col]
            must_cols = [c for c in must_cols if c not in col]

            # get foreign columns
            (
                foreign_must,
                foreign_nice,
                foreign_hash,
            ) = tab_cols(foreign_tab)
            must_cols += foreign_must
            nice_cols += foreign_nice
            if foreign_hash != {}:
                hash_dic.update(foreign_hash)
                hash_dic[col] = foreign_hash[foreign_col]

    # remove duplicates
    must_cols = list(set(must_cols))
    nice_cols = list(set(nice_cols))

    # remove COMMANDS and ['id', 'hash] column
    nice_cols = [
        c
        for c in nice_cols
        if c
        not in SQL_keywords
        + [
            "id",
            "hash",
        ]
    ]
    must_cols = [
        c
        for c in must_cols
        if c
        not in [
            "id",
            "hash",
        ]
    ]
    return (
        must_cols,
        nice_cols,
        hash_dic,
    )


def print_file(file: str):
    try:
        with open(file, "r") as f:
            print(f.read())
    except FileNotFoundError:
        print(f"File {file} not found")


class AbbreviationParser(argparse.ArgumentParser):
    def _get_abbreviation(self, cmd, choices):
        matches = [choice for choice in choices if choice.startswith(cmd)]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            self.error(f"Ambiguous abbreviation '{cmd}', match: {matches}.")
        else:
            self.error(f"No match found for abbreviation '{cmd}'.")

    def parse_args(self, args=None, namespace=None):
        if args is None:
            args = sys.argv[1:]    
        # Check if the first positional argument is an abbreviation of a subcommand
        if args and args[0] in self._subparsers._actions[1].choices:
            return super().parse_args(args, namespace)

        if args and args[0]:
            full_cmd = self._get_abbreviation(args[0], self._subparsers._actions[1].choices)
            args[0] = full_cmd
        return super().parse_args(args, namespace)