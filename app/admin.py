"""
Administrative functions: removal, edit
"""

import sys
from argparse import Namespace

import pandas as pd

from app import sql
from app.common import (
    BOM_PROJECT,
    DEV_HASH,
    DEV_ID,
    SHOP_ID,
    print_file,
    tab_cols,
)
from app.message import MessageHandler
from app.tabs import NA_rows, align_data, prepare_project, tabs_in_data

msg = MessageHandler()


def admin(args: Namespace) -> None:
    """admin methods"""
    ids = []
    if args.align_manufacturers:
        align()
        sys.exit(0)
    if args.config:
        print_file("./conf/config.py")

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
        return


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
    dat = align_data(dat=devs)
    # aborted by user or data aligned
    if dat.empty:
        sys.exit(0)
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
