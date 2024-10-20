import os
import re
import sqlite3
from typing import Dict, List, Union, Set

import pandas as pd

from app.common import read_json, unpack_foreign
from conf.config import db_file, SQL_scheme, SQL_keywords
from app.error import (
    sql_checkError,
    sql_createError,
    read_jsonError,
    sql_executeError,
    sql_getError,
)
from app.error import messageHandler, sql_tabError


msg = messageHandler()

"""manages SQL db.
DB structure is described in ./conf/sql_scheme.json
"""


def tab_exists(tab: str) -> None:
    # check if tab exists!
    sql_scheme = read_json(SQL_scheme)
    if tab not in sql_scheme.keys():
        raise sql_tabError(tab, sql_scheme.keys())


def put(dat: pd.DataFrame, tab: str, on_conflict: list[dict]) -> Dict:
    # put DataFrame into sql at table=tab
    # takes from DataFrame only columns present in sql table
    # check if tab exists!
    # on_conflict is a list of dictionary:
    # [
    #  {
    #  'action': UPDATE SET|REPLACE|NOTHING|FAIL|ABORT|ROLLBACK
    #  'unique_col':[cols] //may be from UNIQUE or PRIMARY KEY
    #  'add_col':[cols] //UPDATE will add new value to existing
    #  }
    # ]
    # put() may raise sql_executeError and sql_tabError
    tab_exists(tab)

    if dat.empty:
        return
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )

    # add new data to sql
    sql_columns = tab_columns(tab)
    d = dat.loc[:, [c in sql_columns for c in dat.columns]]
    return __write_table__(dat=d, tab=tab, on_conflict=on_conflict)


def __write_table__(
    dat: pd.DataFrame, tab: str, on_conflict: str
) -> Dict[str, pd.DataFrame]:
    """writes DataFrame to SQL table 'tab'"""
    records = list(dat.astype("string").to_records(index=False))
    records = [tuple(__escape_quote__(r)) for r in records]

    for conflict in on_conflict:
        action = conflict["action"]
        if action == "UPDATE SET":
            unique_col = conflict["unique_col"]
            cmd_action = f"""ON CONFLICT ({','.join(unique_col)})
                        DO {action}
                    """
            cmd_action += ",".join(
                [f"{c} = {c} + EXCLUDED.{c}" for c in conflict["add_col"]]
            )
            action = ""
        else:
            action = "OR " + action
            cmd_action = ""
            break

    cmd = f"""INSERT {action} INTO {tab} {tuple(dat.columns)}
            VALUES
        """
    cmd += ",".join([str(c) for c in records])
    cmd += cmd_action

    return __sql_execute__([cmd])


def getDF(**kwargs) -> pd.DataFrame:
    """wraper around get() when:
    - search is on one col only
    returns dataframe, in contrast to Dict[col:pd.DataFrame]
    """
    resp = get(**kwargs)
    return list(resp.values())[0] if resp else pd.DataFrame()


def getL(**kwargs) -> List:
    """wraper around get() when:
    - search in on one col only
    - get only one column from DataFrame
    returns list, in contrast to Dict[col:pd.DataFrame]
    """
    resp = get(**kwargs)
    df = list(resp.values())[0]
    return [] if df.empty else list(df.to_dict(orient="list").values())[0]


def get(
    tab: str,
    get: Union[List[str], Set] = ["%"],
    search: Union[List[str], Set] = ["%"],
    where: Union[List[str], Set] = ["%"],
    follow: bool = False,
) -> Dict[str, pd.DataFrame]:
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        tab: table to search
        get: column name to extract (defoult '%' for all columns)
        search: what to get (defoult '%' for everything)
        where: columns used for searching (defoult '%' for everything)
        follow: if True, search in all FOREIGN subtables
    """
    # check if tab exists!
    tab_exists(tab)
    search = __escape_quote__(search)
    sql_scheme = read_json(SQL_scheme)

    if "FOREIGN" not in sql_scheme[tab].keys():
        follow = False

    all_cols = tab_columns(tab)

    if where[0] == "%":
        where = all_cols
    if get[0] == "%" and not follow:
        get = all_cols

    if follow:
        resp = __get_tab__(tab=tab, get=all_cols, search=search, where=where)
        for r in resp:
            base_tab = resp[r]
            if not base_tab.empty:
                for F in sql_scheme[tab]["FOREIGN"]:
                    col, f_tab, f_col = unpack_foreign(F)
                    f_DF = getDF(tab=f_tab)
                    base_tab = base_tab.merge(f_DF, left_on=col, right_on=f_col)
                    if get != ["%"]:
                        if any(c not in base_tab.columns for c in get):
                            raise sql_getError(get, base_tab.columns)
                        base_tab = base_tab[get]
                    resp[r] = base_tab

    else:
        if any(g not in all_cols for g in get):
            raise sql_getError(get, all_cols)
        resp = __get_tab__(tab=tab, get=get, search=search, where=where)

    return resp


def __get_tab__(
    tab: str,
    get: Union[List[str], Set],
    search: Union[List[str], Set],
    where: Union[List[str], Set],
) -> Dict[str, pd.DataFrame]:
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        tab: table to search
        get: column name to extract (defoult '%' for all columns)
        search: what to get (defoult '%' for everything)
        where: columns used for searching (defoult '%' for everything)
    """
    resp = {}

    for c in where:
        cmd = f"SELECT {','.join(get)} FROM {tab} WHERE "
        cmd += "("
        cmd += " ".join([f"{c} LIKE '{s}' OR " for s in search])
        cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
        cmd += ")"
        if resp_col := __sql_execute__([cmd]):
            resp[c] = resp_col[cmd[0:100]].drop_duplicates()
    return resp


def __split_cmd__(script: list) -> List[List]:
    # split OR logic chain into 500 len elements
    # there is limit of 1000 tree depth
    def split_logic_chain(cmd):
        cmd = cmd.replace("\t", "").replace("\n", "").replace("\r", "")
        where_index = cmd.find("WHERE")
        if where_index == -1:
            return [cmd]
        cmd1 = cmd[: where_index + 5]
        cmd2 = cmd[where_index + 5 :]
        cmd3 = cmd2[cmd2.find(")") + 1 :]
        cmd2 = cmd2[: cmd2.find(")") + 1]
        logic_tree = cmd2.replace("'", "stock_name")
        lenOR = logic_tree.count(" OR ")
        lenAND = logic_tree.count(" AND ")
        if lenAND != 0 and lenAND + lenOR > 500:
            raise ValueError("FATAL: sql cmd exceeded length limit")
        if lenOR > 500:
            cmd_new = [cmd1 + c + cmd3 for c in split_list(cmd2, 500)]
            return cmd_new
        else:
            return [cmd]

    def split_list(cmd: str, n) -> list:
        lst = cmd.split(" OR ")
        res = [" OR ".join(lst[i : i + n]) for i in range(0, len(lst), n)]
        res = [r.strip() for r in res]
        # remove first and last parenthesis in string if exists
        res = [r[1:] if r[0] == "(" else r for r in res]
        res = [r[:-2] if r[-1] == ")" else r for r in res]
        return [f" ( {r} ) " for r in res]

    resp = [split_logic_chain(cmd) for cmd in script]
    return [r for r in resp if r]


def __split_list__(lst: str, nel: int) -> list:
    """
    Split list into parts with nel elements each (except last)
    """
    lst_split = re.split(" OR ", lst)
    n = (len(lst_split) // nel) + 1
    cmd_split = [
        " OR ".join(lst_split[i * nel : (i + 1) * nel]) for i in range(n)
    ]
    # make sure each part starts and ends with parenthesis
    cmd_split = ["(" + re.sub(r"[\(\)]", "", s) + ") " for s in cmd_split]
    return cmd_split


def rm(tab: str, value: list[str] = ["%"], column: list[str] = ["%"]) -> None:
    """Remove all instances of value from column in tab"""
    if value == ["%"]:
        column = ["%"]
    for c in column:
        cmd = f"DELETE FROM {tab} "
        if column != ["%"]:
            cmd += 'WHERE ('
            cmd += " ".join([f"{c} LIKE '{s}' OR " for s in value])
            cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
            cmd += ")"
        __sql_execute__([cmd])


def tab_columns(tab: str) -> List[str]:
    """return list of columns for table"""
    sql_cmd = f"pragma table_info({tab})"
    resp = __sql_execute__([sql_cmd])
    if not resp or resp[sql_cmd] is None or "name" not in list(resp[sql_cmd]):
        return []
    return resp[sql_cmd]["name"].to_list()


def sql_check() -> None:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery
    DB location and name taken from ./conf/configuration/py
    """
    # make sure if exists
    if not os.path.isfile(db_file):
        msg.SQL_file_miss(db_file)
        sql_create()

    # check if correct sql
    sql_scheme = read_json(SQL_scheme)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [
            k for k in sql_scheme[tab].keys() if k not in SQL_keywords
        ]
        if tab_columns(tab) != scheme_cols:
            raise sql_checkError(db_file, tab)


def __sql_execute__(script: list) -> Dict[str, pd.DataFrame]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}
    Command is shortened to first 100 characters
    Split cmd if logic tree exceeds 500 (just in case as limit is 1000)
    Also control cmd character number (<100e6) NOT IMPLEMENTED
    will generate error only

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command[0:20]: response in form of pd.DataFrame (may be empty)}
    """
    for c in script:
        if len(c) > 100e6:
            raise sql_executeError("Command too long ", c)

    ans = {}
    cmd = ""
    # when writing panda as dictionary
    # NULL is written as <NA>, sql needs NULL
    script = [re.sub("<NA>", "NULL", str(c)) for c in script]
    # Foreign key constraints are disabled by default,
    # so must be enabled separately for each database connection.
    script = ["PRAGMA foreign_keys = ON"] + script
    script_split = __split_cmd__(script)
    try:
        con = sqlite3.connect(
            db_file,
            detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES,
        )
        cur = con.cursor()
        for cmd_split in script_split:
            cmd = script[script_split.index(cmd_split)]
            cmd100 = cmd[0:100]
            for c in cmd_split:
                cur.execute(c)
                if a := cur.fetchall():
                    colnames = [c[0] for c in cur.description]
                    if cmd in ans:
                        ans[cmd100] = pd.concat(
                            [
                                ans[cmd].fillna(""),
                                pd.DataFrame(a, columns=colnames).fillna(""),
                            ],
                            ignore_index=True,
                        )
                    else:
                        ans[cmd100] = pd.DataFrame(a, columns=colnames)
                else:
                    ans[cmd100] = pd.DataFrame()
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        raise sql_executeError(err, cmd100)
    except sqlite3.Error as err:
        raise sql_executeError(err, cmd100)
    finally:
        cur.close()  # type: ignore
        con.close()  # type: ignore


def sql_create() -> None:
    """Creates sql query based on sql_scheme.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.
    """
    if os.path.isfile(db_file):
        # just in case the file exists
        os.remove(db_file)

    try:
        sql_scheme = read_json(SQL_scheme)
    except read_jsonError as err:
        print(err)
        raise sql_createError(SQL_scheme)

    # create tables query for db
    sql_cmd = []
    for tab in sql_scheme:
        tab_cmd = f"CREATE TABLE {tab} ("
        for col in sql_scheme[tab]:
            if col not in SQL_keywords:
                tab_cmd += f"{col} {sql_scheme[tab][col]}, "
            elif col == "FOREIGN":  # FOREIGN
                for foreign in sql_scheme[tab][col]:
                    k, v = list(foreign.items())[0]
                    tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v}, "
        tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
        tab_cmd += ") "
        sql_cmd.append(tab_cmd)
        if unique_cols := str(
            sql_scheme[tab]["UNIQUE"]
            if "UNIQUE" in sql_scheme[tab].keys()
            else ""
        ):
            # replace square brackets with parenthesis
            unique_cols = re.sub(r"\[", r"(", unique_cols)
            unique_cols = re.sub(r"\]", r")", unique_cols)
            tab_cmd = (
                f"CREATE UNIQUE INDEX uniqueRow_{tab} ON {tab} {unique_cols}"
            )
            sql_cmd.append(tab_cmd)

    # sort sql_cmd list (to make sure you refer to columns that already exists)
    # elements containing "FOREIGN" put at end
    sql_cmd.sort(key=lambda x: x.find("FOREIGN"))
    # and then all UNIQUE INDEX put at end
    sql_cmd.sort(key=lambda x: x.find("UNIQUE"))

    # last command to check if all tables were created
    sql_cmd.append("SELECT tbl_name FROM sqlite_master WHERE type='table'")

    try:
        status = __sql_execute__(sql_cmd)
    except sql_executeError as err:
        os.remove(db_file)
        print(err)
        raise sql_createError(SQL_scheme)

    if sorted(status[sql_cmd[-1][0:100]]["tbl_name"].to_list()) != sorted(
        list(sql_scheme.keys())
    ):
        if os.path.isfile(db_file):
            os.remove(db_file)
        raise sql_createError(SQL_scheme)


def __escape_quote__(txt: Union[List[str], set]) -> List[str]:
    """
    escape quotes in a list of strings
    """
    return [re.sub(r"'", r"''", str(txt)) for txt in txt if txt is not None]
