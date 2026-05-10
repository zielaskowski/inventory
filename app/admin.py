"""
Administrative functions: removal, edit
"""

import os
import sys
from argparse import Namespace

import pandas as pd

import conf.config as conf
from app import sql
from app.common import (
    backup_config,
    create_loc_config,
    display_conf,
    list_backups,
    read_json_list,
    restore_config,
    str_to_date_backup,
    tab_cols,
    write_json,
)
from app.error import ReadJsonError, SqlCreateError, SqlExecuteError, VimdiffSelError
from app.manufacturers import (
    get_alt_man,
    write_alt_man,
)
from app.message import msg
from app.tabs import NA_rows, align_data, prepare_project, tabs_in_data


def admin(args: Namespace) -> None:  # pylint: disable=R0912
    """admin methods"""
    ids = []
    if args.align_manufacturers:
        align()
        return
    if args.display_config:
        print("config from: " + os.path.join(conf.CONFIG_PATH, "config.py"))
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
        sql.rm(tab="SHOP", value=ids, column=[conf.SHOP_ID])
        print(f"removed {len(ids)} devices")
        sys.exit(1)
    if args.remove_project:
        remove_project(args)
    if args.sql_upgrade:
        upgrade(force=args.force)
    if args.import_manufacturers:
        import_manufacturers(
            file=args.import_manufacturers,
            force=args.force,
        )
    if args.export_manufacturers:
        export_manufacturers(args.export_manufacturers)
    if args.backup_config:
        backup_config()
    if args.restore_config:
        select_restore_backup()
    if args.undo:
        select_log_undo(args.undo)


def upgrade(force=False) -> None:
    """
    upgrade sql db to latest standard
    """
    backup_config()
    try:
        sql.sql_upgrade(force=force)
        import_manufacturers(conf.MAN_ALT)
    except (SqlCreateError, KeyError) as err:
        restore_config()
        if str(err):
            print(err)
        sys.exit(1)


def select_log_undo(n: int):
    """undo commands
    from log selected to last one
    """
    logs = sql.log.log_read(n)
    if logs.empty:
        msg.msg("nothing to undo.")
        sys.exit(1)
    log_no = msg.select_log(logs)
    undo_date = logs.loc[log_no, conf.LOG_DATE]
    sql.undo(undo_date)


def select_restore_backup():
    """ask which and then restore backup"""
    lb = list_backups()
    if lb == []:
        msg.msg("no backups. abort.")
        sys.exit(0)
    b_date = []
    for b in lb:
        b_dt = str_to_date_backup(b)
        b_date.append(b_dt.strftime("%Y-%m-%d %H:%M"))
    idx = msg.select_backup(b_date)
    restore_config(idx)


def export_manufacturers(file: str) -> None:
    """export manufacturers to a file"""
    alt_man = get_alt_man()
    write_json(file=file, content=alt_man)
    msg.msg(f"Exported data to {file}")


def import_manufacturers(file: str, force=False):
    """import manufacturers from json to sql
    if force==True, first remove what in db and import
    """
    try:
        alt_exist = read_json_list(file)
        if force:
            sql.rm("ALTERNATIVE_MANUFACTURER")
            sql.rm("MANUFACTURER")
        write_alt_man(alt_exist)
        msg.msg(f"imported manufacturer alternatives from '{file}'")
    except (ReadJsonError, SqlCreateError, SqlExecuteError) as e:
        msg.msg(str(e))
        msg.msg("skipping importing manufacturers")


def remove_project(args: Namespace) -> None:
    """
    Remove from BOM table based on match on project column
    """
    if (projects := prepare_project(projects=args.remove_project)) == []:
        return
    sql.rm(tab="BOM", value=projects, column=[conf.BOM_PROJECT])
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
        project_devs = sql.getL(tab="BOM", get_col=[conf.DEV_ID], follow=True)
        dev = [d for d in dev if d not in project_devs]
    else:
        # unless you Forced to remove whole projects with device_id
        project_tab = sql.getDF(
            tab="BOM",
            get_col=[conf.BOM_PROJECT, conf.DEV_ID],
            search=dev,
            where=[conf.DEV_ID],
            follow=True,
        )
        project_tab = list(set(project_tab[conf.BOM_PROJECT].tolist()))
        sql.rm(tab="BOM", value=project_tab, column=[conf.BOM_PROJECT])

    dev_hash = sql.getL(
        tab="DEVICE", get_col=[conf.DEV_HASH], search=dev, where=[conf.DEV_ID]
    )

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
    except VimdiffSelError as e:
        msg.msg(str(e))
        sys.exit(0)
    # aborted by user or data aligned
    if dat.empty:
        sys.exit(1)
    # remove old hashes of changed devices
    # only if hash changed, other way will update
    # updating (not replacing) is critical to allow undo operation
    changed_hash = dat["hash"] != dat["dev_rm"]
    sql.rm_all_tabs(hash_list=dat.loc[changed_hash, "dev_rm"].to_list())
    # write aligned data back to SQL
    tabs = tabs_in_data(dat)
    for t in tabs:
        must_cols, nice_cols = tab_cols(t)
        tab_dat = NA_rows(dat, must_cols, nice_cols, inform=False)
        on_conflict = None
        sql.put(dat=tab_dat, tab=t, on_conflict=on_conflict)
