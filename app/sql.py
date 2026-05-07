"""sql user managing functions
manages SQL db.
DB structure is described in ./conf/sql_scheme.json
"""

import json
import os
import sys
from typing import Dict, Iterator, List, Set, cast

import pandas as pd
from _pytest._code import source

import conf.config as conf
from app import sql_core as sql
from app.common import (
    TypedItertuple,
    norm_to_list_str,
    tab_exists_scheme,
    unpack_foreign,
)
from app.error import (
    SqlCheckError,
    SqlExecuteError,
)
from app.log import log
from app.message import msg


def sql_upgrade() -> None:
    """
    add sql auditing
    add manufacturers table and try to import from manufacturer_alternatives.json
    add LOG table
    add UNIQUE key to STOCK table
    """
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
        sql.__create__(t)

    # copying tables (inside defer_tables())is not preserving triggers
    # anyway better to rebuild all
    for t in [
        t
        for t in sql.sql_scheme.keys()
        if t not in conf.SQL_KEYWORDS and t in sql.__list_tables__()
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
    # trick for pyright to understand pandas itertuples
    rows = cast(
        Iterator[TypedItertuple],
        logs.itertuples(index=False, name="TypedItertuple"),
    )
    for r in rows:
        args = json.loads(r.data)
        col_defs = sql.__table_definition__(r.source)
        pks = [c for c in col_defs.keys() if "PRIMARY" in col_defs[c]]
        if "created" in r.type:
            try:
                rm(
                    tab=r.source,  # type: ignore
                    value=[r.subject],  # type: ignore
                    column=pks,
                )
            except SqlExecuteError as e:
                # DEVICE tables are replaced when adding new devices
                # to allow columns alignment. This triggers DEVICE.created audit.
                # Removing may violate FOREIGN KEY constraint
                if "FOREIGN KEY" in str(e):
                    continue
                msg.msg(str(e))
        if "deleted" in r.type:
            dat = pd.DataFrame(args["old"], index=pd.Series([0]))
            put(dat=dat, tab=r.source)
        if "updated" in r.type:
            changed_cols = [
                nk for nk, nv in args["new"].items() if nv != args["old"][nk]
            ]
            for c in changed_cols:
                edit(
                    tab=r.source,
                    new_val=[args["old"][c]],
                    col=c,
                    search=[r.subject],
                    where=pks[0],
                )
    log.log_on = log_on


def put(dat: pd.DataFrame, tab: str, on_conflict: dict | None = None) -> Dict:
    """
    put DataFrame into sql at table=tab
    takes from DataFrame only columns present in sql table
    check if tab exists!
    If 'on_conflict' is None, use one defined in sql_scheme.json,
    other way take 'action' (so using UPDATE_SET with add_columns not implemented)
    raises:
        SqlExecuteError and SqlTabError
    """
    log.log_write()
    tab_exists_scheme(tab)

    if dat.empty:
        return {}
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )
    # define action on conflict
    # on_conflict is a list of dictionary defined in sql_scheme.jsonc
    if on_conflict is None:
        on_conflict = sql.sql_scheme[tab].get("ON_CONFLICT", {})
    # add new data to sql
    sql_columns = sql.__tab_columns__(tab)
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
        where=[conf.BOM_HASH],
    )
    shop_tab = getDF(
        tab="SHOP",
        search=hash_list,
        where=[conf.SHOP_HASH],
    )
    stock_tab = getDF(
        tab="STOCK",
        search=hash_list,
        where=[conf.STOCK_HASH],
    )
    if not bom_tab.empty:
        dat = pd.merge(
            left=dat,
            right=bom_tab,
            left_on=merge_on,
            right_on=conf.BOM_HASH,
            how="left",
        ).drop(
            columns=[conf.BOM_HASH, conf.SHOP_HASH, conf.STOCK_HASH], errors="ignore"
        )
    if not shop_tab.empty:
        dat = pd.merge(
            left=dat,
            right=shop_tab,
            left_on=merge_on,
            right_on=conf.SHOP_HASH,
            how="left",
        ).drop(
            columns=[conf.BOM_HASH, conf.SHOP_HASH, conf.STOCK_HASH], errors="ignore"
        )
    if not stock_tab.empty:
        dat = pd.merge(
            left=dat,
            right=stock_tab,
            left_on=merge_on,
            right_on=conf.STOCK_HASH,
            how="left",
        ).drop(
            columns=[conf.BOM_HASH, conf.SHOP_HASH, conf.STOCK_HASH], errors="ignore"
        )
    return dat


def rm_all_tabs(hash_list: list[str]) -> None:
    """remove from all tabs per hash list"""
    log.log_write()
    tabs = list(sql.sql_scheme.keys())
    tab_hash = []
    for t in tabs:
        th, _, _ = unpack_foreign(sql.sql_scheme[t].get("FOREIGN"))
        tab_hash.append(th)
    for table, hash_col in {
        "BOM": conf.BOM_HASH,
        "SHOP": conf.SHOP_HASH,
        "STOCK": conf.STOCK_HASH,
        "DEVICE": conf.DEV_HASH,
    }.items():
        rm(tab=table, value=hash_list, column=[hash_col])


def getDF(  # pylint: disable=R0913,R0917,C0103
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
    resp = sql.__get__(
        tab=tab, get_col=get_col, search=search, where=where, follow=follow, oper=oper
    )
    return list(resp.values())[0] if resp else pd.DataFrame()


def getL(  # pylint: disable=R0913,R0917,C0103
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
    resp = sql.__get__(
        tab=tab, get_col=get_col, search=search, where=where, follow=follow, oper=oper
    )
    df = list(resp.values())[0]
    return [] if df.empty else list(df.to_dict(orient="list").values())[0]


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


def check() -> None:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery
    DB location and name taken from ./conf/config.py
    """
    # make sure if exists
    if not os.path.isfile(conf.DB_FILE):
        msg.sql_file_miss(conf.DB_FILE)
        sql.__create__()

    for tab in sql.sql_scheme.keys():
        scheme_cols = [
            k for k in sql.sql_scheme[tab].keys() if k not in conf.SQL_KEYWORDS
        ]
        # check if correct tables in sql
        if not any(c in sql.__tab_columns__(tab) for c in scheme_cols):
            raise SqlCheckError(conf.DB_FILE, tab)
        # check if correct foreign keys
        from_sql_scheme = [str(c) for c in sql.__tab_foreign__(tab)]
        from_json_scheme = [str(c) for c in sql.sql_scheme[tab].get("FOREIGN", [])]
        if sorted(from_sql_scheme) != sorted(from_json_scheme):
            raise SqlCheckError(conf.DB_FILE, tab)
        # check if foreign keys DEFERRED
        tab_sql = sql.__sqlite_master__(tab)
        if "FOREIGN" in tab_sql and "DEFERRABLE" not in tab_sql:
            raise SqlCheckError(conf.DB_FILE, tab, foreign=True)
        # in old db, STOCK table was missing UNIQUE directive (was handled by ON CONFLICT)
        # now must be declared explicitly
        if not sql.__sqlite_master__(f"uniqueRow_{tab}") and tab == "STOCK":
            raise SqlCheckError(conf.DB_FILE, tab, unique=True)
    # check auditing tables (audite_changefeed)
    if "audite_changefeed" not in sql.__list_tables__():
        raise SqlCheckError(conf.DB_FILE, "audite")
