import os
import re
import sqlite3
from datetime import date
from typing import Dict, List, Union, Set

import pandas as pd

from app.common import read_json
from conf.config import db_file, SQL_scheme
from app.error import sql_checkError, sql_createError, read_jsonError, sql_executeError
from app.error import messageHandler


msg = messageHandler()

"""manages SQL db.
DB structure is described in ./conf/sql_scheme.json
"""


def query(
    db_file: str,
    tab: str,
    symbol: List[str],
    from_date: Union[None, date],
    to_date: Union[None, date],
) -> pd.DataFrame:
    """get data from sql db about symbol,
    including relevant data from description tab
    (GEO tab treated separately)
    Usefull to outlook the data avaliable
    Returns all columns (except hash)

    Args:
        db_file: db file
        tab: table in sql
        symbol: symbol from table:
        from_date: start date of data (including). if missing take only last date
        to_date: last date of data (including). If empty string, return only last available
    """
    if not sql_check(db_file):
        return pd.DataFrame()
    symbol = __escape_quote__(set(symbol))

    if tab == "GEO":
        desc = ""
        se_cols = ["country", "iso2", "region"]
    else:
        desc = "_DESC"
        se_cols = ["symbol"]

    # get tab columns (without hash)
    cols = tab_columns(tab=tab, db_file=db_file)
    cols += tab_columns(tab=tab + desc, db_file=db_file)
    columns_txt = ",".join({c for c in cols if c != "hash"})

    cmd = f"""SELECT {columns_txt}
	        FROM {tab+desc} td"""
    if tab != "GEO":
        cmd += f" INNER JOIN {tab} t ON t.hash=td.hash"
    cmd += " WHERE "
    cmd += "("
    for c in se_cols:
        cmd += "".join([f"td.{c} LIKE '" + s + "' OR " for s in symbol])
    cmd += f"td.{se_cols[0]} LIKE 'none' "  # just to finish last OR
    cmd += ")"

    if tab != "GEO":
        if to_date and from_date:
            cmd += f"""AND strftime('%s',date) BETWEEN
                        strftime('%s','{from_date}') AND strftime('%s','{to_date}')
                    """

        else:
            cmd += r"AND strftime('%s',t.date)=strftime('%s',td.to_date)"
    resp = __sql_execute__([cmd], db_file)
    if resp is None or resp[cmd].empty:
        return pd.DataFrame()
    resp = resp[cmd]
    if tab == "STOCK":
        idx = stock_index(db_file=db_file, search=resp.loc[:, "symbol"].to_list())
        if idx is not None:
            resp = resp.merge(idx, how="left", on="symbol")
    return resp.drop_duplicates()


def index_components(db_file: str, search: List) -> List[str]:
    """
    Return components of given index name.
    """
    search = __escape_quote__(search)
    cmd = (
        """SELECT
                s.symbol
            FROM
                STOCK_DESC s
            INNER JOIN INDEXES_DESC i ON i.hash=c.indexes_hash
            INNER JOIN COMPONENTS c on s.hash = c.stock_hash
                WHERE 
        """
        + "("
    )
    cmd += "".join(["i.name LIKE '" + s + "' OR " for s in search])
    cmd += "i.name LIKE 'none' "  # just to finish last OR
    cmd += ")"
    resp = __sql_execute__([cmd], db_file=db_file)
    return [] if resp is None or resp[cmd].empty else resp[cmd]["symbol"].to_list()


def stock_index(db_file: str, search: List) -> Union[pd.DataFrame, None]:
    """
    Return index(es) where stock belongs.
    Use stock symbol in search
    Return DataFrame with columns [indexes] and [stock]
    """
    search = __escape_quote__(search)
    cmd = (
        """SELECT
                i.name AS 'indexes', s.symbol AS 'symbol'
            FROM
                STOCK_DESC s
            INNER JOIN INDEXES_DESC i ON i.hash=c.indexes_hash
            INNER JOIN COMPONENTS c on s.hash = c.stock_hash
                WHERE 
        """
        + "("
    )
    cmd += "".join(["s.symbol LIKE '" + s + "' OR " for s in set(search)])
    cmd += "s.symbol LIKE 'none' "  # just to finish last OR
    cmd += ")"
    resp = __sql_execute__([cmd], db_file=db_file)
    if resp is None or resp[cmd].empty:
        return
    return resp[cmd]


def currency_of_country(db_file: str, country: Union[List, set]) -> pd.DataFrame:
    """
    Return currency info for country
    """
    cmd = """SELECT *
            FROM CURRENCY_DESC cd
            INNER JOIN GEO g ON cd.symbol=g.currency
                WHERE
        """
    cmd += "("
    cmd += "".join(["g.iso2 LIKE '" + c + "' OR " for c in country])
    cmd += "g.iso2 LIKE 'none' "  # just to finish last OR
    cmd += ")"
    resp = __sql_execute__([cmd], db_file=db_file)
    if resp is None or resp[cmd].empty:
        return pd.DataFrame()
    return resp[cmd].drop(["currency", "last_upd", "hash"], axis="columns")


def currency_rate(db_file: str, dat: pd.DataFrame) -> pd.DataFrame:
    """
    Return currency rate for cur_symbol | date
    """
    min_date = dat.date.min()
    max_date = dat.date.max()
    cmd = [
        f"""SELECT c.val, c.date, cd.symbol
            FROM CURRENCY c
            INNER JOIN CURRENCY_DESC cd ON c.hash=cd.hash
                WHERE
            (cd.symbol LIKE '{symbol}' AND strftime('%s',c.date) BETWEEN
                    strftime('%s','{min_date}') AND strftime('%s','{max_date}') )
            """
        for symbol in dat["symbol"].drop_duplicates()
    ]
    resp = __sql_execute__(cmd, db_file=db_file)
    if resp is None or resp[cmd[0]].empty:
        return pd.DataFrame()
    return pd.concat(resp.values(), ignore_index=True)


def tab_exists(tab: str) -> bool:
    # check if tab exists!
    sql_scheme = read_json(SQL_scheme)
    if tab not in sql_scheme.keys():
        print("Wrong table name. Existing tables:")
        print(list(sql_scheme.keys()))
        return False
    return True


def put(dat: pd.DataFrame, tab: str, on_conflict: str) -> Union[Dict, None]:
    # put DataFrame into sql at table=tab
    # takes from DataFrame only columns present in sql table
    # check if tab exists!
    # on_conflict: '+' | 'replace' | 'ignore'
    if not tab_exists(tab):
        return
    if dat.empty:
        return
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )

    sql_scheme = read_json(SQL_scheme)
    tabL = [f"{tab}_DESC", tab] if f"{tab}_DESC" in sql_scheme.keys() else [tab]

    # ON CONFLICT (unique_col1, unique_col2) DO UPDATE SET col = EXCLUDED.col

    # add new data to sql
    for t in tabL:
        sql_columns = tab_columns(t, db_file)
        d = dat.loc[:, [c in sql_columns for c in dat.columns]]
        resp = __write_table__(
            dat=d,
            tab=t,
            db_file=db_file,
        )
        if not resp:
            return

    return {"put": "success"}


def __write_table__(
    dat: pd.DataFrame, tab: str, db_file: str
) -> Union[None, Dict[str, pd.DataFrame]]:
    """writes DataFrame to SQL table 'tab'"""
    records = list(dat.astype("string").to_records(index=False))
    records = [tuple(__escape_quote__(r)) for r in records]
    cmd = [
        f"""INSERT OR REPLACE INTO {tab} {tuple(dat.columns)}
            VALUES {values_list}
        """
        for values_list in records
    ]
    return __sql_execute__(cmd, db_file)


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
    db_file: str,
    tab: str,
    get: Union[List[str], Set] = ["%"],
    search: Union[List[str], Set] = ["%"],
    where: Union[List[str], Set] = ["%"],
) -> Dict[str, pd.DataFrame]:  # sourcery skip: default-mutable-arg
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        db_file: file location
        tab: table to search
        get: column name to extract (defoult '%' for all columns)
        search: what to get (defoult '%' for everything)
        where: columns used for searching (defoult '%' for everything)
    """
    search = __escape_quote__(search)
    resp = {}
    all_cols = tab_columns(tab=tab, db_file=db_file)
    tab = tab.upper()
    search = [s.upper() for s in search]
    get = [g.lower() for g in get]
    where = [c.lower() for c in where]
    if where[0] == "%":
        where = all_cols
    if get[0] == "%":
        get = all_cols
    if any(g not in all_cols for g in get):
        print(f"Not correct get='{get}' argument.")
        print(f"possible options: {all_cols}")
        return {}

    # check if tab exists!
    if not tab_exists(tab):
        return {}

    for c in where:
        cmd = f"SELECT {','.join(get)} FROM {tab} WHERE "
        cmd += "("
        cmd += " ".join([f"{c} LIKE '{s}' OR " for s in search])
        cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
        cmd += ")"
        if resp_col := __sql_execute__([cmd], db_file):
            resp[c] = resp_col[cmd].drop_duplicates()
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
    cmd_split = [" OR ".join(lst_split[i * nel : (i + 1) * nel]) for i in range(n)]
    # make sure each part starts and ends with parenthesis
    cmd_split = ["(" + re.sub(r"[\(\)]", "", s) + ") " for s in cmd_split]
    return cmd_split


def rm_all(tab: str, symbol: str, db_file: str) -> Union[None, Dict[str, pd.DataFrame]]:
    """
    Remove all instances to asset
    remove from given tab, from tab+_DESC and from COMPONENTS
    (so don't use tab='TAB_DESC'!)
    """
    symbol = symbol.upper()
    tab = tab.upper()
    # check if tab exists!
    if not tab_exists(tab):
        return
    hashes = getL(
        tab=f"{tab}_DESC",
        get=["hash"],
        search=[symbol],
        where=["symbol"],
        db_file=db_file,
    )[0]

    cmd = [f"DELETE FROM {tab} WHERE hash='{hashes}'"]
    cmd += [f"DELETE FROM COMPONENTS WHERE stock_hash='{hashes}'"]
    cmd += [f"DELETE FROM {tab}_DESC WHERE hash='{hashes}'"]

    return __sql_execute__(cmd, db_file)


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
        msg.SQL_file_missing(db_file)
        sql_create()

    # check if correct sql
    sql_scheme = read_json(SQL_scheme)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [
            k
            for k in sql_scheme[tab].keys()
            if k not in ["FOREIGN", "UNIQUE", "HASH_COLS"]
        ]
        if tab_columns(tab) != scheme_cols:
            raise sql_checkError(db_file, tab)


def __sql_execute__(script: list) -> Dict[str, pd.DataFrame]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}
    Split cmd if logic tree exceeds 500 (just in case as limit is 1000)

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame (may be empty)}
    """
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
            db_file, detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES
        )
        cur = con.cursor()
        for cmd_split in script_split:
            cmd = script[script_split.index(cmd_split)]
            for c in cmd_split:
                cur.execute(c)
                if a := cur.fetchall():
                    colnames = [c[0] for c in cur.description]
                    if cmd in ans:
                        ans[cmd] = pd.concat(
                            [
                                ans[cmd].fillna(""),
                                pd.DataFrame(a, columns=colnames).fillna(""),
                            ],
                            ignore_index=True,
                        )
                    else:
                        ans[cmd] = pd.DataFrame(a, columns=colnames)
                else:
                    ans[cmd] = pd.DataFrame()
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        raise sql_executeError(err, cmd)
    except sqlite3.Error as err:
        raise sql_executeError(err, cmd)
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
            if col not in ["FOREIGN", "UNIQUE", "HASH_COLS"]:
                tab_cmd += f"{col} {sql_scheme[tab][col]}, "
            elif col == "FOREIGN":  # FOREIGN
                for foreign in sql_scheme[tab][col]:
                    k, v = list(foreign.items())[0]
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
    except sql_executeError as err:
        os.remove(db_file)
        print(err)
        raise sql_createError(SQL_scheme)

    if sorted(status[sql_cmd[-1]]["tbl_name"].to_list()) != sorted(
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
