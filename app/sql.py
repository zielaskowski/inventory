import os
import re
import sqlite3
from typing import Any, Dict, Iterable, List, Sequence, Set, Union

import pandas as pd

import conf.config as conf
from app.common import (
    BOM_HASH,
    DEV_HASH,
    SHOP_HASH,
    SQL_KEYWORDS,
    STOCK_HASH,
    read_json_dict,
    tab_exists,
    unpack_foreign,
)
from app.error import (
    CheckDirError,
    ReadJsonError,
    SqlCheckError,
    SqlCreateError,
    SqlExecuteError,
    SqlGetError,
    SqlSchemeError,
    SqlTabError,
)
from app.message import MessageHandler

msg = MessageHandler()

"""manages SQL db.
DB structure is described in ./conf/sql_scheme.json
"""


def put(dat: pd.DataFrame, tab: str) -> Dict:
    """
    put DataFrame into sql at table=tab
    takes from DataFrame only columns present in sql table
    check if tab exists!
    put() may raise sql_executeError and sql_tabError
    """
    tab_exists(tab)
    sql_scheme = read_json_dict(conf.SQL_SCHEME)

    if dat.empty:
        return {}
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )
    # define action on conflict
    # on_conflict is a list of dictionary defined in sql_scheme.jsonc
    on_conflict = sql_scheme[tab].get("ON_CONFLICT", {})
    # add new data to sql
    sql_columns = tab_columns(tab)
    # take only columns applicable to table
    d = dat.loc[:, [c in sql_columns for c in dat.columns]]
    return __write_table__(dat=d, tab=tab, on_conflict=on_conflict)


def __write_table__(
    dat: pd.DataFrame, tab: str, on_conflict: dict
) -> Dict[str, pd.DataFrame]:
    """writes DataFrame to SQL table 'tab'"""
    # Create a copy to avoid modifying the original DataFrame
    df_copy = dat.copy()

    # Convert boolean columns to integers (0 for False, 1 for True)
    for col in df_copy.select_dtypes(include=["bool"]).columns:
        df_copy[col] = df_copy[col].astype(int)

    # Replace pandas' NA/NaN with None for SQL compatibility
    df_copy = df_copy.where(pd.notna(df_copy), None)

    # Convert to list of tuples, which executemany handles correctly
    records = list(df_copy.itertuples(index=False, name=None))

    action = on_conflict.get("action", "REPLACE")  # default is ON CONFLICT REPLACE
    if action == "UPDATE SET":
        unique_col = on_conflict["unique_col"]
        update_cols = ",".join(
            [f"{c} = {c} + EXCLUDED.{c}" for c in on_conflict["add_col"]]
        )
        cmd = f"""INSERT INTO {tab} {tuple(dat.columns)}
                  VALUES ({','.join(['?'] * len(dat.columns))})
                  ON CONFLICT ({','.join(unique_col)})
                  DO UPDATE SET {update_cols}
               """
    else:
        action = "OR " + action
        cmd = f"""INSERT {action} INTO {tab} {tuple(dat.columns)}
                  VALUES ({','.join(['?'] * len(dat.columns))})
               """

    return __sql_execute__([cmd], records)


def getDF_other_tabs(
    dat: pd.DataFrame, hash_list: list[str], merge_on: str
) -> pd.DataFrame:
    """
    get data from all tabs except DEVICE
    merge data on 'merge_on' column (col will be preserved)
    then drop table specific hash columns
    ffill missing values (for multiple projects using the same dev)
    return DataFrame
    """
    bom_tab = getDF(
        tab="BOM",
        search=hash_list,
        where=[BOM_HASH],
    )
    shop_tab = getDF(
        tab="SHOP",
        search=hash_list,
        where=[SHOP_HASH],
    )
    stock_tab = getDF(
        tab="STOCK",
        search=hash_list,
        where=[STOCK_HASH],
    )
    if not bom_tab.empty:
        dat = pd.merge(
            left=dat,
            right=bom_tab,
            left_on=merge_on,
            right_on=BOM_HASH,
            how="left",
        ).drop(columns=[BOM_HASH, SHOP_HASH, STOCK_HASH], errors="ignore")
    if not shop_tab.empty:
        dat = pd.merge(
            left=dat,
            right=shop_tab,
            left_on=merge_on,
            right_on=SHOP_HASH,
            how="left",
        ).drop(columns=[BOM_HASH, SHOP_HASH, STOCK_HASH], errors="ignore")
    if not stock_tab.empty:
        dat = pd.merge(
            left=dat,
            right=stock_tab,
            left_on=merge_on,
            right_on=STOCK_HASH,
            how="left",
        ).drop(columns=[BOM_HASH, SHOP_HASH, STOCK_HASH], errors="ignore")
    return dat


def rm_all_tabs(hash_list: list[str]) -> None:
    """remove from all tabs per hash list"""
    sql_schem = read_json_dict(conf.SQL_SCHEME)
    tabs = list(sql_schem.keys())
    tab_hash = []
    for t in tabs:
        th, _, _ = unpack_foreign(sql_schem[t].get("FOREIGN"))
        tab_hash.append(th)
    for table, hash_col in {
        "BOM": BOM_HASH,
        "SHOP": SHOP_HASH,
        "STOCK": STOCK_HASH,
        "DEVICE": DEV_HASH,
    }.items():
        rm(tab=table, value=hash_list, column=[hash_col])


def getDF(
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
) -> pd.DataFrame:
    """wraper around get() when:
    - search is on one col only
    returns dataframe, in contrast to Dict[col:pd.DataFrame]
    Args:
        tab: table to search
        get: column name to extract (default '%' for all columns)
        search: what to get (default '%' for everything)
        where: columns used for searching (default '%' for everything)
        follow: if True, search in all FOREIGN subtables
    """
    resp = get(tab=tab, get_col=get_col, search=search, where=where, follow=follow)
    return list(resp.values())[0] if resp else pd.DataFrame()


def getL(
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
) -> List:
    """wraper around get() when:
    - search in on one col only
    - get only one column from DataFrame
    returns list, in contrast to Dict[col:pd.DataFrame]
    Args:
        tab: table to search
        get: column name to extract (default '%' for all columns)
        search: what to get (default '%' for everything)
        where: columns used for searching (default '%' for everything)
        follow: if True, search in all FOREIGN subtables
    """
    resp = get(tab=tab, get_col=get_col, search=search, where=where, follow=follow)
    df = list(resp.values())[0]
    return [] if df.empty else list(df.to_dict(orient="list").values())[0]


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


def get(
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
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
        follow: if True, search in all FOREIGN subtables
    """
    # check if tab exists!
    tab_exists(tab)
    # assign values if default
    all_cols = tab_columns(tab)
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

    sql_scheme = read_json_dict(conf.SQL_SCHEME)

    if "FOREIGN" not in sql_scheme[tab].keys():
        follow = False

    if follow:
        # get tab DF and all that follow
        base_tab = getDF(tab=tab)
        for f in sql_scheme[tab].get("FOREIGN", []):
            col, f_tab, f_col = unpack_foreign(f)
            f_df = getDF(tab=f_tab)
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
        resp = __get_tab__(tab=tab, get_col=get_col, search=search, where=where)

    return resp


def __get_tab__(
    tab: str,
    get_col: set[str],
    search: list[str] | None,
    where: set[str],
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
    """
    resp = {}
    for c in where:
        if not search:
            cmd = f"SELECT {','.join(get_col)} FROM {tab}"
        else:
            # Create a placeholder string for the IN clause
            placeholders = ",".join("?" * len(search))
            cmd = f"SELECT {','.join(get_col)} FROM {tab} WHERE {c} IN ({placeholders})"
        if resp_col := __sql_execute__([cmd], search):
            resp[c] = resp_col[cmd].drop_duplicates()
    return resp


def rm(
    tab: str,
    value: list[str] | pd.Series | None = None,
    column: list[str] | pd.Series | None = None,
) -> None:
    """
    Remove all instances of value from column in tab.
    if value or column is missing, default is all
    """
    if column is None or value is None:
        cmd = f"DELETE FROM {tab}"
        __sql_execute__([cmd])
        return

    value = norm_to_list_str(value)
    column = norm_to_list_str(column)
    for c in column:
        placeholders = ",".join("?" * len(value))
        cmd = f"DELETE FROM {tab} WHERE {c} IN ({placeholders})"
        __sql_execute__([cmd], value)


def tab_columns(tab: str) -> set[str]:
    """return list of columns for table"""
    sql_cmd = f"pragma table_info({tab})"
    resp = __sql_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return set()
    return set(resp[sql_cmd]["name"].to_list())


def tab_foreign(tab: str) -> List[Dict[str, str]]:
    """show FOREIGN keys for tab"""
    sql_cmd = f"pragma foreign_key_list({tab})"
    resp = __sql_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return []
    foreign_tab = []
    for _, r in resp[sql_cmd].iterrows():
        key = {}
        key[r["from"]] = r["table"] + "(" + r["to"] + ")"
        foreign_tab.append(key)
    return foreign_tab


def sql_check() -> None:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery
    DB location and name taken from ./conf/configuration/py
    """
    # make sure if exists
    if not os.path.isfile(conf.DB_FILE):
        msg.sql_file_miss(conf.DB_FILE)
        sql_create()

    sql_scheme = read_json_dict(conf.SQL_SCHEME)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [k for k in sql_scheme[tab].keys() if k not in SQL_KEYWORDS]
        # check if correct tables in sql
        if not any(c in tab_columns(tab) for c in scheme_cols):
            raise SqlCheckError(conf.DB_FILE, tab)
        # check if correct foreign keys
        from_sql_scheme = [str(c) for c in tab_foreign(tab)]
        from_json_scheme = [str(c) for c in sql_scheme[tab].get("FOREIGN", [])]
        if sorted(from_sql_scheme) != sorted(from_json_scheme):
            raise SqlCheckError(conf.DB_FILE, tab)


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
        con = sqlite3.connect(
            conf.DB_FILE,
            detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES,
        )
        cur = con.cursor()
        for cmd in script:
            if executemany:
                cur.executemany(cmd, params)
            else:
                cur.execute(cmd, params)
            if a := cur.fetchall():
                colnames = pd.Series([c[0] for c in cur.description])
                ans[cmd] = pd.DataFrame(a, columns=colnames)
            else:
                ans[cmd] = pd.DataFrame()
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        raise SqlExecuteError(err, cmd) from err
    except sqlite3.Error as err:
        raise SqlExecuteError(err, cmd) from err
    finally:
        cur.close()  # type: ignore
        con.close()  # type: ignore


def sql_create() -> None:
    """Creates sql query based on sql_scheme.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.
    """
    if os.path.isfile(conf.DB_FILE):
        # just in case the file exists
        os.remove(conf.DB_FILE)

    path = os.path.dirname(conf.DB_FILE)
    if not os.path.isdir(path):
        raise CheckDirError(directory=path)

    try:
        sql_scheme = read_json_dict(conf.SQL_SCHEME)
    except ReadJsonError as err:
        print(err)
        raise SqlCreateError(conf.SQL_SCHEME) from err

    # create tables query for db
    sql_cmd = []
    for tab in sql_scheme:
        tab_cmd = f"CREATE TABLE {tab} ("
        for col in sql_scheme[tab]:  # fmt: off
            if not any(
                [
                    isinstance(sql_scheme[tab], dict),
                    isinstance(sql_scheme[tab], list),
                ]
            ):
                raise SqlSchemeError(tab=tab)
            if col not in SQL_KEYWORDS:  # fmt: on
                tab_cmd += f"{col} {sql_scheme[tab][col]}, "
            elif col == "FOREIGN":  # FOREIGN
                for foreign in sql_scheme[tab][col]:
                    k, v = list(foreign.items())[0]
                    # sqlite do not complain on missing foreign table
                    # during creation
                    try:
                        _, f_table, _ = unpack_foreign(foreign)
                        tab_exists(f_table)
                    except SqlTabError as err:
                        msg.msg(str(err))
                        raise SqlCreateError(conf.SQL_SCHEME) from err

                    tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v}, "
        tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
        tab_cmd += ") "
        sql_cmd.append(tab_cmd)
        if unique_cols := str(
            sql_scheme[tab]["UNIQUE"] if "UNIQUE" in sql_scheme[tab].keys() else ""
        ):
            # replace square brackets with parenthesis
            unique_cols = re.sub(r"\[", r"(", unique_cols)
            unique_cols = re.sub(r"\]", r")", unique_cols)
            tab_cmd = f"CREATE UNIQUE INDEX uniqueRow_{tab} ON {tab} {unique_cols}"
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
    except SqlExecuteError as err:
        os.remove(conf.DB_FILE)
        msg.msg(str(err))
        raise SqlCreateError(conf.SQL_SCHEME) from err

    if sorted(status[sql_cmd[-1][0:100]]["tbl_name"].to_list()) != sorted(
        list(sql_scheme.keys())
    ):
        if os.path.isfile(conf.DB_FILE):
            os.remove(conf.DB_FILE)
        raise SqlCreateError(conf.SQL_SCHEME)


def __escape_quote__(txt: list[str]) -> list[str]:
    """
    escape quotes in a list of strings
    """
    return [re.sub(r"'", r"''", str(txt)) for txt in txt if txt is not None]
