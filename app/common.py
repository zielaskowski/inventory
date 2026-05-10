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
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, List, NamedTuple, Set, Tuple, Union

import pandas as pd

import conf.config as conf
from app.error import (
    AmbigousMatchError,
    CheckDirError,
    NoMatchError,
    ReadJsonError,
    ScanDirPermissionError,
    SqlTabError,
    WriteJsonError,
)
from app.message import msg


class TypedItertuple(NamedTuple):
    """to allow pyright understand pandas itertuple"""

    id: int
    source: str
    subject: str
    type: str
    time: int
    specversion: str
    data: str


def create_loc_config():
    """
    create local config:
        add .config folder
        copy there config.py file
    """
    conf_dir = os.path.join(os.getcwd(), ".config")
    os.makedirs(conf_dir, exist_ok=True)
    glob_conf = os.path.join(conf.MODULE_PATH, "conf", conf.TOML_FILE)
    dest_conf = os.path.join(conf_dir, conf.TOML_FILE)
    shutil.copy2(glob_conf, dest_conf)
    msg.msg(f"created local config in '{dest_conf}'")
    conf.write_TOML(conf_dir)


def backup_config() -> None:
    """
    create backup copy of .config
    """
    backup_suffix = "backup_" + datetime.now().strftime("%Y-%m-%d-%H%M%S%f")
    backup_path = os.path.join(conf.CONFIG_PATH, backup_suffix)
    config_files = [f.path for f in os.scandir(conf.CONFIG_PATH) if f.is_file()]
    config_files = [
        f
        for f in config_files
        if f
        in [
            conf.DB_FILE,
            os.path.join(conf.CONFIG_PATH, conf.TOML_FILE),
            conf.MAN_ALT,
        ]
    ]
    os.mkdir(backup_path)
    for f in config_files:
        shutil.copy2(f, backup_path)

    msg.msg(f"created backup copy of config files in '{backup_path}'")
    msg.msg(f"copied following files: {[os.path.basename(f) for f in config_files]}")


def list_backups() -> list:
    """list backup sub-folders in config folder"""
    backup_dirs = [d.path for d in os.scandir(conf.CONFIG_PATH) if d.is_dir()]
    backup_dirs = [d for d in backup_dirs if os.path.basename(d).startswith("backup")]
    backup_dirs.sort()
    return backup_dirs


def restore_config(idx=-1) -> None:
    """
    restore backup sql DB
    """
    config_files = [f.path for f in os.scandir(list_backups()[idx]) if f.is_file()]
    for f in config_files:
        shutil.copy2(f, conf.CONFIG_PATH)
    msg.msg(f"Backup files restored: {config_files}")


def int_to_date_log(logi_date: int) -> str:
    """convert log date to string
    from format %s to %Y-%b-%d %H:%M:%S
    """
    logi_dt = datetime.fromtimestamp(logi_date)
    return logi_dt.strftime("%Y-%b-%d %H:%M:%S")


def str_to_date_backup(date: str) -> datetime:
    """convert string date to datetime object
    date in formt path/.config/backup_%Y-%b-%d-%h%M%s%f"""
    pattern = re.compile(
        r"""
                         ^
                         (?P<year>\d{4})-
                         (?P<month>\d{2})-
                         (?P<day>\d{2})-
                         (?P<hour>\d{2})
                         (?P<minute>\d{2})
                         (?P<sec>\d{2})
                         (?P<microsec>\d+)
                         $
                         """,
        re.VERBOSE,
    )
    date = os.path.basename(date)
    date = date.replace("backup_", "")
    match = pattern.match(date)
    if not match:
        return datetime.now()
    date_yr = int(match["year"])
    date_month = int(match["month"])
    date_day = int(match["day"])
    time_hrs = int(match["hour"])
    time_min = int(match["minute"])
    time_sec = int(match["sec"])
    return datetime(
        date_yr,
        date_month,
        date_day,
        time_hrs,
        time_min,
        time_sec,
    )


def display_conf() -> None:
    """
    Display configuration
    """
    conf_txt = conf.read_TOML(os.path.join(conf.CONFIG_PATH, conf.TOML_FILE))
    for var, val in conf_txt.items():
        print(var + ": " + str(val))


def first_diff_index(list1: list[str], list2: list[str]) -> int:
    """return index of first different element in lists"""
    for i, (x, y) in enumerate(zip(list1, list2)):
        if x != y:
            return i + 1
    if len(list1) != len(list2):
        return min(len(list1), len(list2))
    return 0  # identical lists


def write_json(file: str, content: dict[str, dict] | dict[str, list[str]]) -> None:
    """
    write content to a file in json format
    overwrite existing file
    """
    try:
        with open(file, mode="w", encoding="UTF8") as f:
            json.dump(content, f)
    except JSONDecodeError as err:
        raise WriteJsonError(file) from err


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
        raise ReadJsonError(file) from err

    try:
        data = json.loads(json_f)
        if not all(isinstance(v, dict) for v in data.values()):
            raise ReadJsonError(file, type_val="dictionary")
    except JSONDecodeError as err:
        raise ReadJsonError(file) from err
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
        raise ReadJsonError(file) from err

    try:
        data = json.loads(json_f)
        if not all(isinstance(v, list) for v in data.values()):
            raise ReadJsonError(file, type_val="list")
    except JSONDecodeError as err:
        raise ReadJsonError(file) from err
    return data


def find_files(directory: str, file_format: str) -> list:
    """
    scan all subdirectories (starting from 'directory')
    search for any xlsx or xls file, only in BOM folder
    in case format=='csv', search for *.csv files
    return a list of full path+file_name
    """
    if not os.access(directory, os.W_OK):
        raise ScanDirPermissionError(directory=directory)
    console_width = shutil.get_terminal_size().columns
    match_files = []
    file_ext = conf.import_format[file_format]["file_ext"]
    s_dir = conf.SCAN_DIR.upper()
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
            if cur_dir == s_dir or (conf.INCLUDE_SUB_DIR and s_dir in folder.upper()):
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
    # expanding the path will mke search include the current path
    # find_files (in particular os.walk()) ignores '.' and '..'
    args.dir = os.path.abspath(args.dir)
    if not os.path.exists(args.dir):
        raise CheckDirError(directory=args.dir, scan_dir=conf.SCAN_DIR)
    files = find_files(args.dir, args.format)

    # filter by file name
    if args.file is not None:
        files = [f for f in files if args.file in os.path.basename(f)]
        if files == []:
            raise CheckDirError(
                file=args.file,
                directory=args.dir,
                project=getattr(args, "project", args.file),
                scan_dir=conf.SCAN_DIR,
            )
    return files


def tab_in_scheme() -> List:
    """return list of tables in sql_scheme.json
    raises:
        read_jsonError
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    return list(sql_scheme.keys())


def list_to_tuple_str(*args: Union[List, pd.Index], quote=True) -> str:
    """convert list to string
    without(!!) parenthesis around. Add quotes around columns only if quote==True
    Simple str(tuple) with one element adds comma at the end
    which brakes sql, and not always parenthesis are valid
    """
    lst = []
    for a in args:
        if isinstance(a, pd.Index):
            a = list(a)
        lst += a
    out = str(lst)
    out = out.replace("[", "").replace("]", "")
    if not quote:
        out = out.replace("'", "").replace('"', "")
    return out


def tab_exists_scheme(tab: str) -> None:
    """
    check if tab exists
    raises:
        sql_tabError if tab not exists
        read_jsonError
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    if tab not in sql_scheme.keys():
        raise SqlTabError(tab, sql_scheme.keys())


def tab_cols(
    tab: str,
    all_cols: bool = False,
    foreign: bool = True,
) -> tuple[list[str], list[str]]:
    """
    return list of columns that are required for the given tab
    and list of columns that are "nice to have"
    follow FOREIGN key constraints to other tab if foreign==True
    if all_cols==True, show also hidden cols
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    tab_exists_scheme(tab)  # will raise sql_tabError if not

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

    if "FOREIGN" in cols and foreign:
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
    if not all_cols:
        # remove COMMANDS which are filled automatically
        nice_cols = [
            c for c in nice_cols if c not in conf.SQL_KEYWORDS + conf.HIDDEN_COLS
        ]
        must_cols = [c for c in must_cols if c not in conf.HIDDEN_COLS]
    else:
        nice_cols = [c for c in nice_cols if c not in conf.SQL_KEYWORDS]
        if "id" in must_cols:
            must_cols.remove("id")
    return (must_cols, nice_cols)


def foreign_tabs(tab: str) -> list[str]:
    """return list of tables referenced in FOREIGN key"""
    tab_exists_scheme(tab)  # will raise sql_tabError if not
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    tabs = []
    for k in sql_scheme[tab].get("FOREIGN", []):
        _, f_tab, _ = unpack_foreign(k)
        tabs += [f_tab]
    return tabs


def refernce_foreign(tab: str) -> Tuple[set[str], List[str]]:
    """Return colnames of tab which are related by any
    other table. Return column name referenced by other tables
    foreign keys and foreign key itself for reporting purposes
    """
    tab_exists_scheme(tab)  # will raise sql_tabError if not
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    tabs = tab_in_scheme()
    tabs = [t for t in tabs if t != tab]
    ref_cols = []
    ref_f = []
    for t in tabs:
        for f in sql_scheme[t].get("FOREIGN", {}):
            ref_col, ref_tab, ref_tab_col = unpack_foreign(f)
            if ref_tab == tab:
                ref_cols.append(ref_tab_col)
                ref_f.append(
                    {t + "(" + ref_col + ") : " + tab + "(" + ref_tab_col + ")"}
                )
    return set(ref_cols), ref_f


def unpack_foreign(foreign: dict[str, str] | None | list) -> tuple[str, str, str]:
    """
    read foreign key from sql_scheme
    expected input:
        sql_scheme[tab].get('FOREIGN') -> list[dict] | None
        for foreign in sql_scheme[tab].get('FOREIGN',{}) -> dict
    tab without FOREIGN key will return ('hash',"",""); for example DEVICE tab
    returns col which is connected and destination table and column
    """
    # DEVICE is very special: do not have FOREIGN key
    if foreign is None:
        return "hash", "", ""
    if isinstance(foreign, list):
        foreign_dict = foreign[0]
    else:
        foreign_dict = foreign
    col = list(foreign_dict.keys())[0]
    # get foreign table and column
    f_tab_col = list(foreign_dict.values())[0]

    # get foreign_tab
    (
        f_tab,
        f_col,
    ) = f_tab_col.split("(")
    f_col = f_col.replace(")", "")
    return col, f_tab, f_col


def norm_to_list_str(
    norm: List[str] | List[int] | List[bool] | Set[str] | pd.Series,
) -> list[str]:
    """
    normalize to list of string
    remove duplicates
    also check if all elements are str (rise ValueError)
    """
    if isinstance(norm, pd.Series):
        out = list(norm.astype(str).to_list())
    else:
        out = norm
    # Convert boolean columns to integers (0 for False, 1 for True)
    out = [int(s) if isinstance(s, bool) else s for s in out]

    return [str(s) for s in out]


def match_from_list(cmd: str, choices: Dict | list) -> str:
    """
    try match cmd in list. Return full cmd if only one match.
    Other way return raises no_matchError or ambiguos_matchError
    """
    if cmd in choices:
        return cmd
    matches = [choice for choice in choices if choice.startswith(cmd)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise AmbigousMatchError(cmd=cmd, matches=matches)
    raise NoMatchError(cmd=cmd)
