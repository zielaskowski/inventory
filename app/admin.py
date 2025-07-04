import sys
from argparse import Namespace

import pandas as pd

from app import sql
from app.common import (
    BOM_HASH,
    BOM_PROJECT,
    DEV_HASH,
    DEV_ID,
    DEV_MAN,
    get_alternatives,
    print_file,
    read_json_dict,
)
from app.tabs import NA_rows, align_data, hash_tab, tabs_in_data
from conf import config as conf


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
        dev = remove_dev(ids, "device_id", args.force)
        print(f"removed {len(dev)} devices")
        sys.exit(1)

    if args.remove_shop_id is not False:
        if not ids:
            ids = args.remove_shop_id
        if ids == []:
            print("No items to remove.")
            sys.exit(1)
        sql.rm(tab="SHOP", value=ids, column=["shop_id"])
        print(f"removed {len(ids)} devices")
        sys.exit(1)


# def remove_shop(dev: list[str], by: str, force: bool) -> None:
#     # remove device from shop based shop_id
#     rm(tab='SHOP', value=dev, column=['shop_id'])


def remove_dev(dev: list[str], by: str, force: bool) -> list[str]:
    """
    remove device based on hash or device_id
    include all other tables where device is used
    skip devices present in projects (unless forced)
    return removed devices (not present in project)
    """
    sql_scheme = read_json_dict(conf.SQL_SCHEME)

    # do not delete device if present in PROJECT table
    if not force:
        project_devs = sql.getL(tab="BOM", get_col=[by], follow=True)
        dev = [d for d in dev if d not in project_devs]
    else:
        # unless you Forced to remove whole projects with device_id
        # NOT TESTED!!
        project_tab = sql.getDF(tab="BOM", get_col=[BOM_PROJECT, by], follow=True)
        project_tab = project_tab[project_tab[by].isin(dev)]
        project_tab = list(set(project_tab[BOM_PROJECT].tolist()))
        sql.rm(tab="BOM", value=project_tab, column=[BOM_PROJECT])

    dev_id = sql.getL(tab="DEVICE", get_col=[DEV_ID], search=dev, where=[by])

    all_tabs = list(sql_scheme.keys())
    # put DEVICE table on very end
    all_tabs.remove("DEVICE")
    all_tabs.append("DEVICE")
    for t in all_tabs:
        sql.rm(tab=t, value=dev_id, column=[DEV_ID])
    return dev


def apply_alternatives() -> None:
    """
    apply alternatives stored
    must remove first to not add qty to bom
    """
    dat = sql.getDF(tab="DEVICE")
    dat.loc[:, DEV_MAN], differ_rows = get_alternatives(dat[DEV_MAN].to_list())
    dat = dat.loc[differ_rows, :]
    # store hashes for later removal
    dat["dev_rm"] = dat[DEV_HASH]
    # add data from other tabs
    dat = sql.getDF_other_tabs(
        dat=dat,
        hash_list=dat["dev_rm"].to_list(),
        merge_on="dev_rm",
    )
    # rehash for new manufacturers
    dat = hash_tab(dat)
    # remove old
    sql.rm_all_tabs(hash_list=dat["dev_rm"].to_list())
    # add new
    tabs = tabs_in_data(dat)
    for t in tabs:
        sql.put(dat=dat, tab=t)


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
    # aborted by user
    if dat.empty:
        sys.exit(0)
    # remove old hashes of changed devices
    sql.rm_all_tabs(hash_list=dat["dev_rm"].to_list())
    # write aligned data back to SQL
    tabs = tabs_in_data(dat)
    for t in tabs:
        if t == "BOM":
            sql.put(dat=dat.drop_duplicates(subset=[DEV_HASH, BOM_PROJECT]), tab=t)
            continue
        sql.put(dat=dat, tab=t)
