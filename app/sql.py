"""sql user managing functions
manages SQL db.
DB structure is described in ./conf/sql_scheme.json
"""

import json
import os
import re
from argparse import Namespace
from datetime import datetime
from enum import unique
from typing import Dict, List, Set

import pandas as pd

from app import sql_core as sql
from app.common import (
    int_to_date_log,
    read_json_dict,
    tab_exists_scheme,
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
from app.message import msg
from conf.config import *  # pylint: disable=unused-wildcard-import,wildcard-import


class Log:
    """loging methods"""

    def __init__(self) -> None:
        # log only once, each logging by sql_execute will change to FALSE
        # one command usualy trigger mutliple sql operation
        self.log_on = True
        self.cmd = []

    def log(self, args: Namespace) -> None:
        """
        log command in ./conf/log.txt
        """
        if LOG_FILE == "":
            return

        self.cmd = ["python -m inv"]
        for var, val in vars(args).items():
            if callable(val):
                continue
            if val:
                if var == "command":
                    self.cmd += [val]
                else:
                    if isinstance(val, list):
                        val = " ".join(f"'{v}'" for v in val)
                    self.cmd += [
                        "--"
                        + var
                        + " "
                        + (str(val) if not isinstance(val, bool) else "")
                    ]

    def log_read(self, n: int) -> pd.DataFrame:
        """
        read form log
        can rise IsDirectoryError and FileNotFoundError
        split dates from commands and return as tuple
        """
        try:
            logs = getDF(tab="LOG")
            if logs.empty:
                return logs
        except (SqlGetError, SqlExecuteError) as e:
            msg.msg(str(e))
            sys.exit(1)
        n = min(n, len(logs))
        logs = logs.loc[len(logs) - n : len(logs), :]
        # reverse index so easier to select
        logs.sort_values(by=LOG_DATE, inplace=True, ascending=False)
        logs.reset_index(inplace=True, drop=True)
        logs.sort_values(by=LOG_DATE, inplace=True)
        logs.reset_index(inplace=True)
        logs["id"] = logs["index"].apply(lambda x: x + 1)
        logs["date_fmt"] = logs["date"].apply(int_to_date_log)
        return logs

    def log_write(self, force=False) -> None:
        """
        write to log only once (unless force==True)
        can rise IsDirectoryError and FileNotFoundError
        """
        if not self.log_on and not force:
            return
        self.log_on = False
        now = datetime.now()
        now = now.strftime("%s")
        try:
            put(
                tab="LOG",
                dat=pd.DataFrame(
                    {LOG_DATE: now, LOG_ARGS: " ".join(self.cmd)},
                    index=[1],  # pyright: ignore
                ),
            )
        except (SqlExecuteError, SqlTabError) as e:
            msg.msg(str(e))
            sys.exit(1)


log = Log()


def sql_upgrade() -> None:
    """
    add sql auditing
    add manufacters table and try to import from manufacturer_alternatives.json
    add LOG table
    add UNIQE key to STOCK table
    """
    sql_scheme = read_json_dict(SQL_SCHEME)
    missing_tabs = [
        t
        for t in [
            "MANUFACTURER",
            "ALTERNATIVE_MANUFACTURER",
            "LOG",
        ]
        if t not in sql.__list_tables__()
    ]
    # upadate also if audit is missing
    audit_tab = [t for t in ["audite_changefeed"] if t not in sql.__list_tables__()]
    defer_tabs = []
    for t in sql.__list_tables__():
        tab_sql = sql.__sqlite_master__(t)
        if "FOREIGN" in tab_sql and "DEFERRABLE" not in tab_sql:
            defer_tabs.append(t)

    # old definition of STOCK table was missing UNIQE key
    if not sql.__sqlite_master__(name="uniqueRow_STOCK"):
        defer_tabs.append("STOCK")

    if not missing_tabs and not defer_tabs and not audit_tab:
        msg.msg("sql DB already in latest version")
        sys.exit(1)

    for t in defer_tabs:
        sql.__defer_foreign__(tab=t)
        # after commit sql will remove redundant UNIQE keys
        # and defer_foreign delete old and create new tab
        sql.__add_unique__(tab=t)
    for t in missing_tabs:
        create(t)

    # copying tables (inside defer_tables())is not preserving triggers
    # anyway better to rebuild all
    for t in [
        t
        for t in sql_scheme.keys()
        if t not in SQL_KEYWORDS and t in sql.__list_tables__()
    ]:
        sql.__audit__(t)

    msg.sql_upgrade()


def undo(from_date: int) -> None:
    """undo all commands from date"""
    logs = getDF(
        tab="audite_changefeed",
        search=[from_date],
        where=["time"],
        oper=">=",
    )
    # undo starting from last command
    logs.sort_index(ascending=False, inplace=True)
    # do not log now
    log_on = log.log_on
    log.log_on = False
    # TODO: list undo logs so user have chance to undo undo
    for r in logs.itertuples():
        args = json.loads(r.data)  # type: ignore
        if "created" in r.type:  # type: ignore
            args = args["new"]
            col_defs = sql.__table_definition__(r.source)  # type: ignore
            pks = [c for c in col_defs.keys() if "PRIMARY" in col_defs[c]]
            pk = pks[0]
            qty_cols = [str(c) for c in args.keys() if c in [BOM_QTY, STOCK_QTY]]
            if qty_cols != []:
                # check if we updated (added qty) or created new record
                qty_col: str = qty_cols[0]
                curr_row = getDF(
                    tab=r.source,  # type: ignore
                    get_col=qty_cols,
                    search=[args[pk]],
                    where=pks,
                )
                curr_qty = int(curr_row.loc[0, qty_col])
                log_qty = args[qty_col]
            else:
                log_qty = 0
                curr_qty = 0
                qty_col = ""
            try:
                if not curr_qty - log_qty:  # change was whole qty, remove
                    rm(
                        tab=r.source,  # type: ignore
                        value=[r.subject],  # type: ignore
                        column=pks,
                    )
                else:  # change was only qty, update
                    edit(
                        tab=r.source,  # type: ignore
                        new_val=[curr_qty - log_qty],  # type: ignore
                        col=qty_col,
                        search=[args[pk]],
                        where=pk,
                    )
            except SqlExecuteError as e:
                if "FOREIGN KEY" not in str(e):
                    sys.exit(1)
    log.log_on = log_on
    return


def write_man_alternatives(man_alt: dict) -> None:
    """store manufacturers into tables"""
    if [k for k in man_alt if k] == []:
        # nothing to do
        msg.msg("empty file")
        raise ReadJsonError(MAN_ALT, type_val="List")
    man = pd.DataFrame(columns=[MAN_NAME, MAN_ALT_NAME])  # pyright: ignore
    for k, val in man_alt.items():
        if not k:
            continue
        for v in val:
            new_row = pd.DataFrame(
                {MAN_NAME: k, MAN_ALT_NAME: v}, index=[0]  # pyright: ignore
            )
            man = pd.concat([man, new_row], ignore_index=True)
    put(dat=pd.DataFrame({MAN_NAME: man[MAN_NAME].unique()}), tab="MANUFACTURER")
    base_man = getDF(tab="MANUFACTURER", search=list(man_alt.keys()), where=[MAN_NAME])
    alt_man = pd.merge(left=man, right=base_man, how="left", on=MAN_NAME)
    put(dat=alt_man, tab="ALTERNATIVE_MANUFACTURER")


def get_man_alternatives() -> dict:
    """get manufacturers from"""
    alt_man = {}
    alt_man_df = getDF("ALTERNATIVE_MANUFACTURER", follow=True)
    if alt_man_df.empty:
        return alt_man
    for m in alt_man_df[MAN_NAME].unique():
        mask = alt_man_df[MAN_NAME] == m
        alt_man[m] = alt_man_df.loc[mask, MAN_ALT_NAME].to_list()
    return alt_man


def store_alternatives(
    alternatives: dict[str, list[str]],
    selection: list[str],
) -> None:
    """
    write selection made by user, so next time no need to choose
    only for DEV_MAN column and only if selection was one-to-one
    """
    if alternatives == {}:
        return
    alt_keys = list(alternatives.keys())
    alt_len = len(alternatives[alt_keys[0]])
    alt_from = alternatives[alt_keys[0]]
    try:
        alt_exist = get_man_alternatives()
    except ReadJsonError as e:
        msg.msg(str(e))
        return

    for i in range(alt_len):
        # only one-to-one alternatives and changed
        if alt_from[i] != selection[i]:
            # remove alternative if already exists
            for k in list(alt_exist.keys()):
                if alt_from[i] in alt_exist[k]:
                    alt_exist[k].remove(alt_from[i])
                    if alt_exist[k] == []:
                        alt_exist.pop(k)
            if selection[i] in alt_exist.keys():
                alt_exist[selection[i]].append(alt_from[i])
                alt_exist[selection[i]] = list(set(alt_exist[selection[i]]))
            else:
                alt_exist[selection[i]] = [alt_from[i]]
    write_man_alternatives(alt_exist)


def get_alternatives(manufacturers: list[str]) -> tuple[list[str], list[bool]]:
    """
    check if we have match from stored alternative
    return list with replaced manufacturers
    (complete list, including not replaced also)
    and list of bools indicating where replaced
    """
    alt_exist = get_man_alternatives()
    man_replaced = []
    for man in manufacturers:
        rep = [k for k, v in alt_exist.items() if man in v]
        if rep != []:
            man_replaced.append(rep[0])
        else:
            man_replaced.append(man)
    # inform user about alternatives (be explicit!)
    alt = pd.DataFrame({"was": manufacturers, "alternative": man_replaced})
    differ_row = alt["was"] != alt["alternative"]
    if not alt.loc[differ_row, :].empty:
        if not msg.inform_alternatives(alternatives=alt.loc[differ_row, :]):
            return manufacturers, []
    return man_replaced, differ_row.to_list()


def put(dat: pd.DataFrame, tab: str, on_conflict: dict | None = None) -> Dict:
    """
    put DataFrame into sql at table=tab
    takes from DataFrame only columns present in sql table
    check if tab exists!
    put() may raise SqlExecuteError and SqlTabError
    """
    log.log_write()
    tab_exists_scheme(tab)
    sql_scheme = read_json_dict(SQL_SCHEME)

    if dat.empty:
        return {}
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )
    # define action on conflict
    # on_conflict is a list of dictionary defined in sql_scheme.jsonc
    if on_conflict is None:
        on_conflict = sql_scheme[tab].get("ON_CONFLICT", {})
    # add new data to sql
    sql_columns = tab_columns(tab)
    # take only columns applicable to table
    d = dat.loc[:, [c in sql_columns for c in dat.columns]]
    if d.empty:
        return {}
    return sql.__write_table__(
        dat=d,
        tab=tab,
        on_conflict=on_conflict,  # pyright: ignore
    )


def getDF_other_tabs(  # pylint: disable=invalid-name
    dat: pd.DataFrame, hash_list: list[str], merge_on: str
) -> pd.DataFrame:
    """
    get data from all tabs except DEVICE
    merge data on 'merge_on' column (col will be preserved)
    then drop table specific hash columns
    fill missing values (for multiple projects using the same dev)
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
    log.log_write()
    sql_schem = read_json_dict(SQL_SCHEME)
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


def getDF(  # pylint: disable=invalid-name
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
    oper="IN",
) -> pd.DataFrame:
    """wraper around get() when:
    - search is on one col
    returns dataframe, in contrast to Dict[col:pd.DataFrame]
    Args:
        tab: table to search
        get: column name to extract (default '%' for all columns)
        search: what to get (default '%' for everything)
        where: columns used for searching (default '%' for everything)
        follow: if True, search in all FOREIGN subtables
    Raises:
        SqlExecuteError
        SqlGetError
        SqlGetOperationError
    """
    resp = get(
        tab=tab, get_col=get_col, search=search, where=where, follow=follow, oper=oper
    )
    return list(resp.values())[0] if resp else pd.DataFrame()


def getL(  # pylint: disable=invalid-name
    tab: str,
    get_col: List[str] | Set[str] | pd.Series | None = None,
    search: List[str] | List[int] | List[bool] | pd.Series | None = None,
    where: List[str] | Set[str] | pd.Series | None = None,
    follow: bool = False,
    oper="IN",
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
    Raises:
        SqlExecuteError
        SqlGetError
        SqlGetOperationError
    """
    resp = get(
        tab=tab, get_col=get_col, search=search, where=where, follow=follow, oper=oper
    )
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


def get(  # pylint: disable=too-many-branches
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
        follow: if True, search in all FOREIGN subtables
    Raises:
        SqlExecuteError
        SqlGetError
        SqlGetOperationError
    """
    # check if tab exists!
    # tab_exists_scheme(tab)
    if tab not in sql.__list_tables__():
        raise SqlTabError(tab=tab, tabs=sql.__list_tables__())
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
        search = sql.__escape_quote__(search)

    sql_scheme = read_json_dict(SQL_SCHEME)

    fk = sql.__table_foreign_keys__(tab)
    if fk.empty:
        # if "FOREIGN" not in sql_scheme[tab].keys():
        follow = False

    if follow:
        # get tab DF and all that follow
        base_tab = getDF(tab=tab)
        if base_tab.empty:
            return {"": pd.DataFrame()}
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
        resp = sql.__get_tab__(
            tab=tab, get_col=get_col, search=search, where=where, oper=oper
        )

    return resp


def rm(
    tab: str,
    value: list[str] | pd.Series | None = None,
    column: list[str] | pd.Series | None = None,
) -> None:
    """
    Remove all instances of value from column in tab.
    if value or column is missing, default is all
    Raises:
        SqlExecuteError
    """
    log.log_write()
    if column is None or value is None:
        cmd = f"DELETE FROM {tab}"
        sql.__cmd_execute__([cmd])
        return

    value = norm_to_list_str(value)
    column = norm_to_list_str(column)
    for c in column:
        placeholders = ",".join("?" * len(value))
        cmd = f"DELETE FROM {tab} WHERE {c} IN ({placeholders})"
        sql.__cmd_execute__([cmd], value)


def edit(
    tab: str,
    new_val: list[str],
    col: str,
    search: list[str],
    where: str,
) -> None:
    """Update table in db"""
    log.log_write()
    if len(new_val) != len(search):
        return
    cmd = []
    for nv, s in zip(new_val, search):
        cmd.append(f"UPDATE {tab} SET {col} = '{nv}' WHERE {where} = '{s}'")
    sql.__cmd_execute__(cmd)


def tab_columns(tab: str) -> set[str]:
    """return list of columns for table"""
    sql_cmd = f"pragma table_info({tab})"
    resp = sql.__cmd_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return set()
    return set(resp[sql_cmd]["name"].to_list())


def tab_foreign(tab: str) -> List[Dict[str, str]]:
    """show FOREIGN keys for tab"""
    sql_cmd = f"pragma foreign_key_list({tab})"
    resp = sql.__cmd_execute__([sql_cmd])
    if not resp or resp[sql_cmd].empty:
        return []
    foreign_tab = []
    for _, r in resp[sql_cmd].iterrows():
        key = {}
        key[r["from"]] = r["table"] + "(" + r["to"] + ")"
        foreign_tab.append(key)
    return foreign_tab


def check() -> None:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery
    DB location and name taken from ./conf/config.py
    """
    # make sure if exists
    if not os.path.isfile(DB_FILE):
        msg.sql_file_miss(DB_FILE)
        create()

    sql_scheme = read_json_dict(SQL_SCHEME)
    for tab in sql_scheme.keys():
        scheme_cols = [k for k in sql_scheme[tab].keys() if k not in SQL_KEYWORDS]
        # check if correct tables in sql
        if not any(c in tab_columns(tab) for c in scheme_cols):
            raise SqlCheckError(DB_FILE, tab)
        # check if correct foreign keys
        from_sql_scheme = [str(c) for c in tab_foreign(tab)]
        from_json_scheme = [str(c) for c in sql_scheme[tab].get("FOREIGN", [])]
        if sorted(from_sql_scheme) != sorted(from_json_scheme):
            raise SqlCheckError(DB_FILE, tab)
        # check if foreign keys DEFERRED
        tab_sql = sql.__sqlite_master__(tab)
        if "FOREIGN" in tab_sql and "DEFERRABLE" not in tab_sql:
            raise SqlCheckError(DB_FILE, tab, foreign=True)
        # in old db, STOCK table was missing UNIQUE directive (was handled by ON CONFLICT)
        # now must be declared explicitly
        if not sql.__sqlite_master__(f"uniqueRow_{tab}") and tab == "STOCK":
            raise SqlCheckError(DB_FILE, tab, unique=True)
    # check auditing tables (audite_changefeed)
    if "audite_changefeed" not in sql.__list_tables__():
        raise SqlCheckError(DB_FILE, "audite")


def create(one_tab="") -> None:  # pylint: disable=too-many-branches
    """Creates sql query based on sql_scheme.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.
    if one_tab!="" create only one tab. Raise SQL create error if tab exists
    """
    if os.path.isfile(DB_FILE) and not one_tab:
        # just in case the file exists
        # when creating only one tab assumption is that we want to add to existng db
        os.remove(DB_FILE)
    path = os.path.dirname(DB_FILE)
    if not os.path.isdir(path):
        raise CheckDirError(directory=path)

    try:
        sql_scheme = read_json_dict(SQL_SCHEME)
    except ReadJsonError as err:
        print(err)
        raise SqlCreateError(SQL_SCHEME) from err

    # create tables query for db
    sql_cmd = []
    if one_tab:
        sql_scheme = {one_tab: sql_scheme[one_tab]}
    for tab in sql_scheme:
        try:
            sql.__check_scheme__(tab=tab, col_def=sql_scheme[tab])
        except SqlSchemeError as err:
            print(err)
            raise SqlCreateError(SQL_SCHEME) from err

        sql_cmd.append(sql.__create_tab_cmd__(tab))

    # sort sql_cmd list (to make sure you refer to columns that already exists)
    # elements containing "FOREIGN" put at end
    sql_cmd.sort(key=lambda x: x.find("FOREIGN"))

    try:
        sql.__cmd_execute__(sql_cmd)
        if one_tab:
            sql.__add_unique__(tab=one_tab)
        else:
            sql.__add_unique__()
    except SqlExecuteError as err:
        os.remove(DB_FILE)
        msg.msg(str(err))
        raise SqlCreateError(SQL_SCHEME) from err

    # check if all tables created
    all_tables = sql.__list_tables__()
    if not all(k in all_tables for k in sql_scheme.keys()):
        if os.path.isfile(DB_FILE):
            os.remove(DB_FILE)
        raise SqlCreateError(SQL_SCHEME)
    # add auditing all changes on all tables
    for tab in sql_scheme:
        sql.__audit__(tab=tab)
