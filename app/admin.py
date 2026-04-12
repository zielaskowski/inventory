"""
Administrative functions: removal, edit
"""

import os
import sys
from argparse import Namespace
from time import strftime

import pandas as pd

from app import sql
from app.common import (
    backup_config,
    create_loc_config,
    display_conf,
    list_backups,
    log_create,
    read_json_list,
    restore_config,
    str_to_date,
    tab_cols,
    write_json,
)
from app.error import ReadJsonError, SqlCreateError
from app.message import MessageHandler
from app.tabs import NA_rows, align_data, prepare_project, tabs_in_data
from conf.config import *  # pylint: disable=unused-wildcard-import,wildcard-import

msg = MessageHandler()


def admin(args: Namespace) -> None:
    """admin methods"""
    ids = []
    if args.align_manufacturers:
        align()
        return
    if args.display_config:
        print("config from: " + os.path.join(CONFIG_PATH, "config.py"))
        display_conf()
        return
    if args.set_local_config:
        create_loc_config()
        return
    if args.csv:
        try:
            df = pd.read_csv(args.csv)
            df = df[df[args.filter_col] == args.filter_val]
            ids = df[args.what_col].tolist()
        except FileNotFoundError as e:
            print(e)
            sys.exit(1)
        except KeyError as e:
            print(e)
            sys.exit(1)

    if args.remove_dev_id:
        if not ids:
            ids = args.remove_dev_id
        dev = remove_dev(ids, args.force)
        msg.msg(f"removed {len(dev)} devices")
        sys.exit(1)

    if args.remove_shop_id is not False:
        if not ids:
            ids = args.remove_shop_id
        sql.rm(tab="SHOP", value=ids, column=[SHOP_ID])
        print(f"removed {len(ids)} devices")
        sys.exit(1)
    if args.remove_project:
        remove_project(args)
    if args.sql_upgrade:
        sql_upgrade()
    if args.import_manufacturers:
        import_manufacturers(file=args.import_manufacturers)
    if args.export_manufacturers:
        export_manufacturers(args.export_manufacturers)
    if args.backup_config:
        backup_config()
    if args.restore_config:
        select_restore_backup()


def select_restore_backup():
    """ask which and then restore backup"""
    lb = list_backups()
    if lb == []:
        msg.msg("no backups. abort.")
        sys.exit(0)
    b_date = []
    for b in lb:
        b = os.path.basename(b)
        b = b.replace("backup_", "")
        b_dt = str_to_date(b)
        b_date.append(b_dt.strftime("%Y-%m-%d %H:%M"))
    idx = msg.select_backup(b_date)
    restore_config(idx)


def export_manufacturers(file: str) -> None:
    """export manufacturers to a file"""
    alt_man = sql.get_man_alternatives()
    write_json(file=file, content=alt_man)
    msg.msg(f"Exported data to {file}")


def import_manufacturers(file: str):
    """import manufacturers from json to sql"""
    try:
        alt_exist = read_json_list(file)
        sql.store_man_alternatives(alt_exist)
        msg.msg(f"imported manufacturer alternatives from '{file}'")
    except (ReadJsonError, SqlCreateError) as e:
        msg.msg(str(e))
        msg.msg("skipping importing manufacturers")


def sql_upgrade() -> None:
    """
    add sql auditing
    add manufacters table and try to import from manufacturer_alternatives.json
    """
    backup_config()
    if "MANUFACTURER" in sql.sql_tables():
        msg.msg("sql DB already in latest version")
        sys.exit(1)
    try:
        sql.sql_create("MANUFACTURER")
        sql.sql_create("ALTERNATIVE_MANUFACTURER")
        log_create()
        import_manufacturers()
    except SqlCreateError as err:
        restore_config()
        print(err)
        sys.exit(1)
    msg.sql_upgrade()


def remove_project(args: Namespace) -> None:
    """
    Remove from BOM table based on match on project column
    """
    if (projects := prepare_project(projects=args.remove_project)) == []:
        return
    sql.rm(tab="BOM", value=projects, column=[BOM_PROJECT])
    msg.bom_remove(projects)


def remove_dev(dev: list[str], force: bool) -> list[str]:
    """
    remove device based on hash or device_id
    include all other tables where device is used
    skip devices present in projects (unless forced)
    return removed devices (not present in project)
    """

    # do not delete device if present in PROJECT table
    if not force:
        project_devs = sql.getL(tab="BOM", get_col=[DEV_ID], follow=True)
        dev = [d for d in dev if d not in project_devs]
    else:
        # unless you Forced to remove whole projects with device_id
        project_tab = sql.getDF(
            tab="BOM",
            get_col=[BOM_PROJECT, DEV_ID],
            search=dev,
            where=[DEV_ID],
            follow=True,
        )
        project_tab = list(set(project_tab[BOM_PROJECT].tolist()))
        sql.rm(tab="BOM", value=project_tab, column=[BOM_PROJECT])

    dev_hash = sql.getL(tab="DEVICE", get_col=[DEV_HASH], search=dev, where=[DEV_ID])

    sql.rm_all_tabs(hash_list=dev_hash)
    return dev


def align() -> None:
    """align manufacturers"""
    # man_grp: all DEVICES group by dev_id and collect possible manufacturers man1 | man2 | etc
    # dat: merge DEVICES with man_grp, for each dev remove man from man_grp
    # dat: leave only rows where man_grp != dev_man
    # display, on each change redo above
    # on each change for each table:
    # - take old dev_hash lines, remove dev_hash and change manufacturer, add again
    # - align all columns before merge
    devs = sql.getDF(tab="DEVICE")
    try:
        dat = align_data(dat=devs)
    except KeyboardInterrupt as e:
        print(e)
        sys.exit(1)
    # aborted by user or data aligned
    if dat.empty:
        sys.exit(1)
    # remove old hashes of changed devices
    sql.rm_all_tabs(hash_list=dat["dev_rm"].to_list())
    # write aligned data back to SQL
    tabs = tabs_in_data(dat)
    for t in tabs:
        must_cols, nice_cols = tab_cols(t)
        tab_dat = NA_rows(dat, must_cols, nice_cols, inform=False)
        on_conflict = None
        if t == "DEVICE":
            on_conflict = {"action": "REPLACE"}
        sql.put(dat=tab_dat, tab=t, on_conflict=on_conflict)
