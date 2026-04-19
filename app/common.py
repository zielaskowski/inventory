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
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, List

from jinja2 import Template

from app.error import (
    AmbigousMatchError,
    CheckDirError,
    NoMatchError,
    ReadJsonError,
    ScanDirPermissionError,
    SqlTabError,
    WriteJsonError,
)
from app.message import MessageHandler
from conf.config import *  # pylint: disable=unused-wildcard-import,wildcard-import

msg = MessageHandler()


def create_loc_config():
    """
    create local config:
        add .config folder
        copy there config.py file
    """
    conf_dir = os.path.join(os.getcwd(), ".config")
    os.makedirs(conf_dir, exist_ok=True)
    glob_conf = os.path.join(MODULE_PATH, "conf", TOML_FILE)
    dest_conf = os.path.join(conf_dir, TOML_FILE)
    shutil.copy2(glob_conf, dest_conf)
    msg.msg(f"created local config in '{dest_conf}'")
    write_TOML(conf_dir)


def backup_config() -> None:
    """
    create backup copy of .config
    """
    backup_suffix = "backup_" + datetime.now().strftime("%Y-%m-%d-%H%M%S%f")
    backup_path = os.path.join(CONFIG_PATH, backup_suffix)
    config_files = [f.path for f in os.scandir(CONFIG_PATH) if f.is_file()]
    config_files = [
        f
        for f in config_files
        if f in [DB_FILE, os.path.join(CONFIG_PATH, TOML_FILE), LOG_FILE, MAN_ALT]
    ]
    os.mkdir(backup_path)
    for f in config_files:
        shutil.copy2(f, backup_path)

    msg.msg(f"created backup copy of config files in '{backup_path}'")
    msg.msg(f"copied following files: {[os.path.basename(f) for f in config_files]}")


def list_backups() -> list:
    """list backup sub-folders in config folder"""
    backup_dirs = [d.path for d in os.scandir(CONFIG_PATH) if d.is_dir()]
    backup_dirs = [d for d in backup_dirs if os.path.basename(d).startswith("backup")]
    backup_dirs.sort()
    return backup_dirs


def restore_config(idx=-1) -> None:
    """
    restore backup sql DB
    """
    config_files = [f.path for f in os.scandir(list_backups()[idx]) if f.is_file()]
    for f in config_files:
        shutil.copy2(f, CONFIG_PATH)
    msg.msg(f"Backup files restored: {config_files}")


def str_to_date_log(logi: str) -> datetime:
    """convert log date to string
    date in format %Y-%b-%d %h:%M:%s
    """
    pattern = re.compile(
        r"""
                         ^
                         (?P<year>\d{4})-
                         (?P<month>\d{2})-
                         (?P<day>\d{2})\s
                         (?P<hour>\d{2}):
                         (?P<minute>\d{2}):
                         (?P<sec>\d{2})
                         $
                         """,
        re.VERBOSE,
    )
    match = pattern.match(logi)
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
    conf = read_TOML(os.path.join(CONFIG_PATH, TOML_FILE))
    for var, val in conf.items():
        print(var + ": " + str(val))


def first_diff_index(list1: list[str], list2: list[str]) -> int:
    """return index of first different element in lists"""
    for i, (x, y) in enumerate(zip(list1, list2)):
        if x != y:
            return i + 1
    if len(list1) != len(list2):
        return min(len(list1), len(list2))
    return 0  # identical lists


def vimdiff_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ref_col: str,
    change_col: str,
    opt_col: str,
    what_differ: str,
    dev_id: str,
    exit_on_change: bool,
    start_line: int = 1,
):
    """
    prepare vimdiff config from template
    and adjust help message to column
    ref_col, change_col, opt_col are files (without extension) which
                                 will be displayed
    alternate_col is the name of column, displayed in help, when
                  equal DEV_MAN will turn on option selection help and options
    dev_id, used during attributes alignment, to inform about device
    exit_on_change if True, will exit vim after each change
                   (to update file context for exmple)
    """
    with open(
        os.path.join(MODULE_PATH, "conf", "vimdiff_help.txt"),
        mode="r",
        encoding="UTF8",
    ) as f:
        help_temp = Template(f.read())
    with open(
        os.path.join(MODULE_PATH, "conf", ".vimrc"),
        mode="r",
        encoding="UTF8",
    ) as f:
        vimrc_temp = Template(f.read())

    substitutions = {
        "START_LINE": start_line,
        "TEMP_DIR": TEMP_DIR,
        "LEFT_NAME": opt_col,
        "RIGHT_NAME": change_col,
        "WHAT_DIFFER": what_differ,
        "DEV_ID": dev_id,
        "REF_COL": ref_col,
        "MULTIPLE_MANUFACTURERS": DEV_MAN == what_differ,
        "EXIT_ON_CHANGE": exit_on_change,
    }
    vimrc_txt = vimrc_temp.render(substitutions)
    help_txt = help_temp.render(substitutions)
    with open(os.path.join(TEMP_DIR, ".vimrc"), mode="w", encoding="UTF8") as f:
        f.write(vimrc_txt)
    with open(os.path.join(TEMP_DIR, "vimdiff_help.txt"), "w", encoding="UTF8") as f:
        f.write(help_txt)


def log_create() -> None:
    """
    remove old file and check if dir exists
    create dir if possible
    raises PermissionError
    """
    if LOG_FILE == "":
        return
    path = os.path.dirname(LOG_FILE)
    if not os.path.exists(path):
        os.makedirs(path)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    with open(LOG_FILE, "w", encoding="UTF8") as f:
        f.close()


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
    file_ext = import_format[file_format]["file_ext"]
    s_dir = SCAN_DIR.upper()
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
            if cur_dir == s_dir or (INCLUDE_SUB_DIR and s_dir in folder.upper()):
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
        raise CheckDirError(directory=args.dir, scan_dir=SCAN_DIR)
    files = find_files(args.dir, args.format)

    # filter by file name
    if args.file is not None:
        files = [f for f in files if args.file in os.path.basename(f)]
        if files == []:
            raise CheckDirError(
                file=args.file,
                directory=args.dir,
                project=getattr(args, "project", args.file),
                scan_dir=SCAN_DIR,
            )
    return files


def unpack_foreign(foreign: dict[str, str] | None | list) -> tuple[str, str, str]:
    """
    read foreign key from sql_scheme
    expected input:
        sql_scheme[tab].get('FOREIGN') -> list[dict] | None
        for foreign in sql_scheme[tab].get('FOREIGN',[]) -> dict
    tab without FOREIGN key will return None: mean DEVICE tab
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


def tab_exists_scheme(tab: str) -> None:
    """
    check if tab exists
    raises sql_tabError if not
    """
    sql_scheme = read_json_dict(SQL_SCHEME)
    if tab not in sql_scheme.keys():
        raise SqlTabError(tab, sql_scheme.keys())


def tab_cols(
    tab: str,
    all_cols: bool = False,
) -> tuple[list[str], list[str]]:
    """
    return list of columns that are required for the given tab
    and list of columns that are "nice to have"
    follow FOREIGN key constraints to other tab
    """
    sql_scheme = read_json_dict(SQL_SCHEME)
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
    if not all_cols:
        # remove COMMANDS which are filled utomatically
        nice_cols = [c for c in nice_cols if c not in SQL_KEYWORDS + HIDDEN_COLS]
        must_cols = [c for c in must_cols if c not in HIDDEN_COLS]
    else:
        nice_cols = [c for c in nice_cols if c not in SQL_KEYWORDS]
        if "id" in must_cols:
            must_cols.remove("id")
    return (must_cols, nice_cols)


def foreign_tabs(tab: str) -> list[str]:
    """return list of tables refrenced in FOREIGN key"""
    tab_exists_scheme(tab)  # will raise sql_tabError if not
    sql_scheme = read_json_dict(SQL_SCHEME)
    tabs = []
    for k in sql_scheme[tab].get("FOREIGN", []):
        _, f_tab, _ = unpack_foreign(k)
        tabs += [f_tab]
    return tabs


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


class AbbreviationParser(argparse.ArgumentParser):
    """override argparser to provide arguments abbrevation"""

    def _get_abbreviation(  # pylint: disable=inconsistent-return-statements
        self,
        cmd: str,
        choices: Dict,
    ) -> str:
        if cmd in ["-h", "--help"]:
            return cmd
        try:
            return match_from_list(cmd=cmd, choices=choices)
        except AmbigousMatchError as err:
            self.error(str(err))
        except NoMatchError as err:
            self.error(str(err))

    def parse_args(self, args: list | None = None, namespace=None):  # type: ignore
        if args is None:
            args = sys.argv[1:]
        # only subcommand given so show help
        if len(args) == 1:
            args += ["-h"]

        # in case choices are None
        try:
            choices = (
                    self # pylint: disable=protected-access
                    ._subparsers
                    ._actions[1]  # pyright: ignore[reportAssignmentType,reportOptionalMemberAccess]
                    .choices
                    ) # fmt: skip
        except AttributeError:
            self.error("No arguments added to argparser")

        # Check if the first positional argument is an abbreviation of a subcommand
        if args and args[0] in choices:
            return super().parse_args(args, namespace)

        if args and args[0]:
            full_cmd = self._get_abbreviation(
                args[0],
                choices,  # pyright: ignore[reportArgumentType]
            )
            args[0] = full_cmd  # pyright: ignore
        return super().parse_args(args, namespace)
