import json
import hashlib
from typing import Dict
from argparse import Namespace
import re
import os
import pandas as pd

from json import JSONDecodeError

from app.error import read_jsonError, check_dirError, hashError
from app.error import messageHandler

msg = messageHandler()


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
    for dir, _, files in os.walk(directory):
        for file in files:
            if dir.split("/")[-1].upper() == "BOM":
                if file.endswith(".xlsx") or file.endswith(".xls"):
                    xlsx_files.append(os.path.join(dir, file))
    return xlsx_files


def check_dir_file(args: Namespace) -> list[str]:
    # check search directory
    if not os.path.exists(args.dir):
        raise check_dirError(args.dir)
    xlsx_files = find_xlsx_files(args.dir)

    # filter by file name
    if args.file is not None:
        xlsx_files = [f for f in xlsx_files if args.file in f]
        if xlsx_files == []:
            raise check_dirError(args.file)
    return xlsx_files


def hash_table(tab: str, dat: pd.Series, cols: list[str]) -> pd.Series:
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


