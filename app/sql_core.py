"""sql core functions
not to be used directly, see sql.py for user functions
"""

import os
import re
import sqlite3
import sys
from typing import Any, Dict, List, Sequence, Set, Union

import pandas as pd
from audite import track_changes

import conf.config as conf
from app.common import (
    norm_to_list_str,
    read_json_dict,
    tab_exists_scheme,
    unpack_foreign,
)
from app.error import (
    CheckDirError,
    ReadJsonError,
    SqlCreateError,
    SqlExecuteError,
    SqlGetError,
    SqlGetOperationError,
    SqlSchemeError,
    SqlTabError,
)
from app.message import msg

try:
    sql_scheme = read_json_dict(conf.SQL_SCHEME)
except ReadJsonError as err:
    print(err)
    sys.exit(1)


def __create__(one_tab="") -> None:  # pylint: disable=too-many-branches
    """Creates sql query based on sql_scheme.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.
    If one_tab!="" create only one tab. Raise SQL create error if tab exists
    """
    if os.path.isfile(conf.DB_FILE) and not one_tab:
        # just in case the file exists
        # when creating only one tab assumption is that we want to add to existing db
        os.remove(conf.DB_FILE)
    path = os.path.dirname(conf.DB_FILE)
    if not os.path.isdir(path):
        raise CheckDirError(directory=path)

    # create tables query for db
    sql_cmd = []
    if one_tab:
        scheme = {one_tab: sql_scheme[one_tab]}
    else:
        scheme = sql_scheme
    for tab in scheme:
        try:
            __check_scheme__(tab=tab, col_def=scheme[tab])
        except SqlSchemeError as err:
            print(err)
            raise SqlCreateError(conf.SQL_SCHEME) from err

        sql_cmd.append(__create_tab_cmd__(tab))

    # sort sql_cmd list (to make sure you refer to columns that already exists)
    # elements containing "FOREIGN" put at end
    sql_cmd.sort(key=lambda x: x.find("FOREIGN"))

    try:
        __cmd_execute__(sql_cmd)
        if one_tab:
            __add_unique__(tab=one_tab)
        else:
            __add_unique__()
    except SqlExecuteError as err:
        os.remove(conf.DB_FILE)
        msg.msg(str(err))
        raise SqlCreateError(conf.SQL_SCHEME) from err

    # check if all tables created
    all_tables = __list_tables__()
    if not all(k in all_tables for k in scheme.keys()):
        if os.path.isfile(conf.DB_FILE):
            os.remove(conf.DB_FILE)
        raise SqlCreateError(conf.SQL_SCHEME)
    # add auditing all changes on all tables
    for tab in scheme:
        __audit__(tab=tab)


def __list_tables__() -> List:
    """
    list all tables in sql db
    """
    sql_cmd = ["SELECT tbl_name FROM sqlite_master WHERE type='table'"]
    try:
        status = __cmd_execute__(sql_cmd)
        return status[sql_cmd[-1][0:100]]["tbl_name"].to_list()
    except SqlExecuteError as err:
        msg.msg(str(err))
        return []


def __tab_columns__(tab: str) -> set[str]:
    """return list of columns for table"""
    sql_cmd = f"pragma table_info({tab})"
    resp = __cmd_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return set()
    return set(resp[sql_cmd]["name"].to_list())


def __sqlite_master__(name: str, pattern: Union[None, str] = None) -> str:
    """return sql column from db master information where name,
    name=table_name -> table dfinition sql command
    name=uniqueRow_{table name} -> uniqe column for table
    match against pattern if given
    """
    cmd = f"SELECT sql FROM sqlite_schema WHERE name = '{name}'"
    resp = __cmd_execute__([cmd])
    resp_df = resp[cmd]
    if resp_df.empty:
        return ""
    resps = str(resp_df.loc[0, "sql"])
    if pattern is not None:
        if match := re.search(pattern, resps):
            resps = match[0]
        else:
            resps = ""
    return resps


def __table_definition__(tab: str) -> dict:
    """return table definition as dicionary
    with column names as keys and type as values
    """
    if tab not in __list_tables__():
        raise SqlTabError(tab=tab, tabs=__list_tables__())
    cmd = f"PRAGMA table_info({tab})"
    resp = __cmd_execute__([cmd])
    resp_df = resp[cmd]
    # fmt: off
    resp_df["notnull"] = ( # type: ignore
            resp_df["notnull"]
            .apply(lambda x: "NOT NULL" if x else "")
            )
    resp_df["pk"] = ( # type: ignore
            resp_df["pk"]
            .apply(lambda x: "PRIMARY KEY" if x else "")
            )
    resp_df["type"] = (
            resp_df[["type", "notnull", "pk"]] # type: ignore
            .apply(tuple,axis=1)
            .str.join(" ")
            )
    # fmt: on
    resp_dict = resp_df.loc[:, ["name", "type"]].to_dict(orient="records")
    return {i["name"]: i["type"] for i in resp_dict}


def __tab_foreign__(tab: str) -> List[Dict[str, str]]:
    """show FOREIGN keys for tab
    return empty list if no foreign key
    """
    sql_cmd = f"pragma foreign_key_list({tab})"
    resp = __cmd_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return []
    foreign_tab = []
    for _, r in resp[sql_cmd].iterrows():
        key = {}
        key[r["from"]] = r["table"] + "(" + r["to"] + ")"
        foreign_tab.append(key)
    return foreign_tab


def __write_table__(
    dat: pd.DataFrame, tab: str, on_conflict: dict
) -> Dict[str, pd.DataFrame]:
    """writes DataFrame to SQL table 'tab'
    Raises:
        SqlExecuteError
    """
    # Create a copy to avoid modifying the original DataFrame
    df_copy = dat.copy()

    # Convert boolean columns to integers (0 for False, 1 for True)
    for col in df_copy.select_dtypes(include=["bool"]).columns:
        df_copy[col] = df_copy[col].astype(int)

    # Replace pandas' NA/NaN with None for SQL compatibility
    df_copy = df_copy.where(pd.notna(df_copy), None)

    # Convert to list of tuples, which executemany handles correctly
    records = list(df_copy.itertuples(index=False, name=None))

    # single element tuple add coma which brake column names
    col_names = f"{list(dat.columns)}"
    col_names = col_names.replace("[", "(")
    col_names = col_names.replace("]", ")")

    action = on_conflict.get("action", "REPLACE")  # default is ON CONFLICT REPLACE
    if action == "UPDATE_SET":
        action = ""
        update_cmd = __update_set__(tab=tab, add_cols=on_conflict.get("add_col", None))
    else:
        action = "OR " + action
        update_cmd = ""

    cmd = f"""INSERT {action} INTO {tab} {col_names}
              VALUES ({','.join(['?'] * len(dat.columns))})
           """
    cmd += update_cmd
    return __cmd_execute__([cmd], records)


def __get_tab__(
    tab: str,
    get_col: set[str],
    search: list[str] | None,
    where: set[str],
    oper="IN",
) -> Dict[str, pd.DataFrame]:
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        tab: table to search
        get_col: column name to extract
        search: what to get, get all if None
        where: columns used for searching
        oper: operator [IN, >, <, <=, >=], common for each search
    Raises:
        SqlExecuteError
        SqlGetOperationError
    """
    resp = {}
    for c in where:
        if not search:
            cmd = f"SELECT {','.join(get_col)} FROM {tab}"
        else:
            # Create a placeholder string for the IN clause
            if oper == "IN":
                placeholders = ",".join("?" * len(search))
            else:
                # check if numeric for operations like >,<,<=,>=
                col_def = __table_definition__(tab=tab)[c]
                if all(i not in col_def for i in ["INTEGER", "REAL"]):
                    raise SqlGetOperationError(col=c, oper=oper)
                placeholders = "?"
            cmd = f"SELECT {','.join(get_col)} FROM {tab} WHERE {c} {oper} ({placeholders})"
        if resp_col := __cmd_execute__([cmd], search):
            resp[c] = resp_col[cmd].drop_duplicates()
    return resp


def __get__(  # pylint: disable=R0917, R0913, R0914, R0912
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
    oper="IN",
) -> Dict[str, pd.DataFrame]:
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        tab: table to search
        get_col: column name to extract (default None for all columns)
        search: what to get (default None for everything)
        where: columns used for searching (default None for everything)
        follow: if True, search in all FOREIGN sub-tables
    Raises:
        SqlExecuteError
        SqlGetError
        SqlGetOperationError
    """
    # check if tab exists!
    # tab_exists_scheme(tab)
    if tab not in __list_tables__():
        raise SqlTabError(tab=tab, tabs=__list_tables__())
    # assign values if default
    all_cols = __tab_columns__(tab)
    # normalize input to list
    if where is None:
        where = all_cols
    else:
        where = set(norm_to_list_str(where))
    if get_col is None:
        get_col = all_cols
    else:
        get_col = set(norm_to_list_str(get_col))
    if search is not None:
        search = norm_to_list_str(search)
        search = __escape_quote__(search)

    if not __tab_foreign__(tab):
        # if "FOREIGN" not in sql_scheme[tab].keys():
        follow = False

    if follow:
        # get tab DF and all that follow
        resp = __get__(tab=tab)
        base_tab = list(resp.values())[0] if resp else pd.DataFrame()

        if base_tab.empty:
            return {"": pd.DataFrame()}
        for f in sql_scheme[tab].get("FOREIGN", []):
            col, f_tab, f_col = unpack_foreign(f)
            resp = __get__(tab=f_tab)
            f_df = list(resp.values())[0] if resp else pd.DataFrame()
            base_tab = base_tab.merge(f_df, left_on=col, right_on=f_col)
        # mock __get_tab__ response
        # key for each column to search and value for DF
        resp = {}
        for w in where:
            if search:
                resp[w] = base_tab.loc[base_tab[w].isin(search), :]
            else:
                resp[w] = base_tab
            if get_col != all_cols:
                if any(c not in resp[w].columns for c in get_col):
                    raise SqlGetError(get_col, resp[w].columns.to_list())
                resp[w] = resp[w].loc[:, list(get_col)]

    else:
        if any(g not in all_cols for g in get_col):
            raise SqlGetError(get_col, all_cols)
        resp = __get_tab__(
            tab=tab, get_col=get_col, search=search, where=where, oper=oper
        )

    return resp


def __cmd_execute__(
    script: List[str],
    params: Sequence[str | tuple[Any, ...] | list[str]] | None = None,
) -> Dict[str, pd.DataFrame]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}
    will generate error only

    Args:
        script (list): list of sql commands to execute
        params : if None or list(str) will be used in sql.execute to replace '?'
                if list[list[str]] will use executemany

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame (may be empty)}
    Raises:
        SqlExecuteError
    """

    ans = {}
    cmd = ""

    executemany = False
    if params is None:
        params = []
    elif not isinstance(params[0], str):
        executemany = True

    if not executemany:
        if len(params) > 900:
            raise SqlExecuteError("Too many parameters: ", str(len(params)))

    try:
        con = sqlite3.connect(  # pylint: disable=E1101
            conf.DB_FILE,
            detect_types=sqlite3.PARSE_COLNAMES  # pylint: disable=E1101
            | sqlite3.PARSE_DECLTYPES,  # pylint: disable=E1101
        )
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        cur.execute("PRAGMA recursive_triggers = OFF")
        for cmd in script:
            if executemany:
                cur.executemany(cmd, params)
            else:
                cur.execute(cmd, params)
            if a := cur.fetchall():
                col_names = pd.Series([c[0] for c in cur.description])
                ans[cmd] = pd.DataFrame(a, columns=col_names)
            else:
                ans[cmd] = pd.DataFrame()
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:  # pylint: disable=E1101
        con.rollback()  # type: ignore
        raise SqlExecuteError(err=err, cmd=cmd, params=params) from err
    except sqlite3.Error as err:  # pylint: disable=E1101
        raise SqlExecuteError(err=err, cmd=cmd, params=params) from err
    finally:
        cur.close()  # type: ignore
        con.close()  # type: ignore


def __create_tab_cmd__(tab: str) -> str:
    """
    craft sql command to create table
    """
    col_def = sql_scheme[tab]
    tab_cmd = f"CREATE TABLE {tab} ("
    for col in col_def:
        if col not in conf.SQL_KEYWORDS:
            tab_cmd += f"{col} {col_def[col]}, "
        elif col == "FOREIGN":
            for foreign in col_def[col]:
                k, v = list(foreign.items())[0]
                # sqlite do not complain on missing foreign table
                # during creation
                try:
                    _, f_table, _ = unpack_foreign(foreign)
                    tab_exists_scheme(f_table)
                except SqlTabError as err:
                    msg.msg(str(err))
                    raise SqlCreateError(conf.SQL_SCHEME) from err

                # foreign check will fail when REPLACE in table, which is default
                # so db is created with foreign keys INITIALLY DEFERRED
                # in this case the integrity fails only at commit only
                tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v} "
                tab_cmd += "DEFERRABLE INITIALLY DEFERRED, "
    tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
    tab_cmd += ") "
    return tab_cmd


def __update_set__(tab: str, add_cols: Union[str, None]) -> str:
    """create ON_CONFLICT UPDATE_SET cmd"""
    cols = __table_definition__(tab=tab)
    if add_cols:
        update_cols = ",".join([f"{c} = {c} + EXCLUDED.{c}" for c in add_cols])
    else:  # replace all columns except primary
        update_cols = ",".join([f"{c} = EXCLUDED.{c}" for c in cols])

    cmd = f"""ON CONFLICT
              DO UPDATE SET {update_cols}
           """
    return cmd


def __add_unique__(tab: Union[str, None] = None) -> None:
    """create UNIQUE index for tables, or selected table if given
    Raises:
        SqlCreateError
    """
    cmd = []
    if not tab:
        tabs = sql_scheme.keys()
    else:
        tabs = [tab]
    for t in tabs:
        # get uniqe cols from sql_scheme
        if "UNIQUE" in sql_scheme[t].keys():
            unique_cols = sql_scheme[t]["UNIQUE"]
        else:
            continue
        unique_cols = str(unique_cols)
        # replace square brackets with parenthesis
        unique_cols = re.sub(r"\[", r"(", unique_cols)
        unique_cols = re.sub(r"\]", r")", unique_cols)
        if unique_cols:
            cmd.append(f"CREATE UNIQUE INDEX uniqueRow_{t} ON {t} {unique_cols}")
    if cmd:
        __cmd_execute__(cmd)


def __defer_foreign__(tab: str) -> None:
    """defer foraign key in existing table"""
    if tab not in __list_tables__():
        raise SqlTabError(tab=tab, tabs=__list_tables__())
    cmd = [f"ALTER TABLE {tab} RENAME TO {tab}_old"]
    cmd.append(__create_tab_cmd__(tab=tab))
    cmd.append(f"INSERT INTO {tab} SELECT * FROM {tab}_old")
    cmd.append(f"DROP TABLE {tab}_old")
    __cmd_execute__(cmd)


def __check_scheme__(tab: str, col_def: Dict) -> None:  # pylint: disable=R0912
    """
    check correctness of sql scheme json file
    """
    # read_json already checks if each table is a dict
    for col in col_def:
        if col not in conf.SQL_KEYWORDS:  # fmt: on
            if not isinstance(col_def[col], str):
                raise SqlSchemeError(tab=tab, key=col, expected="String")
        elif col == "FOREIGN":  # FOREIGN
            if not isinstance(col_def[col], List):
                raise SqlSchemeError(tab=tab, key=col, expected="List")
            for foreign in col_def[col]:
                if not isinstance(foreign, dict):
                    raise SqlSchemeError(tab=tab, key=col, expected="List of Dict")
        elif col in ["UNIQUE", "HASH_COLS"]:
            if not isinstance(col_def[col], List):
                raise SqlSchemeError(tab=tab, key=col, expected="List")
        elif col == "ON_CONFLICT":
            if not isinstance(col_def[col], Dict):
                raise SqlSchemeError(tab=tab, key=col, expected="Dict")
        elif col == "COL_DESCRIPTION":
            if not isinstance(col_def[col], Dict):
                raise SqlSchemeError(tab=tab, key=col, expected="Dict")


def __escape_quote__(txt: list[str]) -> list[str]:
    """
    escape quotes in a list of strings
    """
    return [re.sub(r"'", r"''", str(txt)) for txt in txt if txt is not None]


def __audit__(tab: str) -> None:
    """add loging tables to sql"""
    db = sqlite3.connect(  # pylint: disable=E1101
        conf.DB_FILE,
        detect_types=sqlite3.PARSE_COLNAMES  # pylint: disable=E1101
        | sqlite3.PARSE_DECLTYPES,  # pylint: disable=E1101
    )
    track_changes(db, tables=[tab])
