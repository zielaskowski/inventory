"""sql core functions
not to be used directly, see sql.py for user functions
"""

import re
import sqlite3
from typing import Any, Dict, List, Sequence

import pandas as pd
from audite import track_changes

from app.common import (
    read_json_dict,
    tab_exists_scheme,
    unpack_foreign,
)
from app.error import (
    SqlCreateError,
    SqlExecuteError,
    SqlGetOperationError,
    SqlSchemeError,
    SqlTabError,
)
from app.message import msg
from conf.config import *  # pylint: disable=unused-wildcard-import,wildcard-import


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
    if action == "UPDATE SET":
        unique_col = on_conflict["unique_col"]
        update_cols = ",".join(
            [f"{c} = {c} + EXCLUDED.{c}" for c in on_conflict["add_col"]]
        )
        cmd = f"""INSERT INTO {tab} {col_names}
                  VALUES ({','.join(['?'] * len(dat.columns))})
                  ON CONFLICT ({','.join(unique_col)})
                  DO UPDATE SET {update_cols}
               """
    else:
        action = "OR " + action
        cmd = f"""INSERT {action} INTO {tab} {col_names}
                  VALUES ({','.join(['?'] * len(dat.columns))})
               """

    return __sql_execute__([cmd], records)


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
    sql_scheme = read_json_dict(SQL_SCHEME)
    resp = {}
    for c in where:
        if not search:
            cmd = f"SELECT {','.join(get_col)} FROM {tab}"
        else:
            # Create a placeholder string for the IN clause
            if oper == "IN":
                placeholders = ",".join("?" * len(search))
            else:
                # check if numeric
                col_def = sql_scheme[tab][c]
                if "INTEGER" not in col_def or "REAL" not in col_def:
                    raise SqlGetOperationError(col=c, oper=oper)
                placeholders = "?"
            cmd = f"SELECT {','.join(get_col)} FROM {tab} WHERE {c} {oper} ({placeholders})"
        if resp_col := __sql_execute__([cmd], search):
            resp[c] = resp_col[cmd].drop_duplicates()
    return resp


def __sql_execute__(
    script: list,
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
            DB_FILE,
            detect_types=sqlite3.PARSE_COLNAMES  # pylint: disable=E1101
            | sqlite3.PARSE_DECLTYPES,  # pylint: disable=E1101
        )
        cur = con.cursor()
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
        raise SqlExecuteError(err, cmd) from err
    except sqlite3.Error as err:  # pylint: disable=E1101
        raise SqlExecuteError(err, cmd) from err
    finally:
        cur.close()  # type: ignore
        con.close()  # type: ignore


def __tab_cmd__(tab: str, col_def: Dict) -> str:
    """
    craft sql command to create table
    """
    tab_cmd = f"CREATE TABLE {tab} ("
    for col in col_def:  # fmt: off
        if col not in SQL_KEYWORDS:  # fmt: on
            tab_cmd += f"{col} {col_def[col]}, "
        elif col == "FOREIGN":  # FOREIGN
            for foreign in col_def[col]:
                k, v = list(foreign.items())[0]
                # sqlite do not complain on missing foreign table
                # during creation
                try:
                    _, f_table, _ = unpack_foreign(foreign)
                    tab_exists_scheme(f_table)
                except SqlTabError as err:
                    msg.msg(str(err))
                    raise SqlCreateError(SQL_SCHEME) from err

                tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v}, "
    tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
    tab_cmd += ") "
    return tab_cmd


def __check_scheme__(tab: str, col_def: Dict) -> None:
    """
    check correctness of sql scheme json file
    """
    # read_json already checks if each table is a dict
    for col in col_def:
        if col not in SQL_KEYWORDS:  # fmt: on
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


def __sql_audit__(tab: str) -> None:
    """add loging tables to sql"""
    db = sqlite3.connect(  # pylint: disable=E1101
        DB_FILE,
        detect_types=sqlite3.PARSE_COLNAMES  # pylint: disable=E1101
        | sqlite3.PARSE_DECLTYPES,  # pylint: disable=E1101
    )
    track_changes(db, tables=[tab])
