import sys
from argparse import Namespace

import pandas as pd

from app.common import print_file, read_json
from app.sql import getDF, getL, rm
from conf.config import SQL_SCHEME


def admin(args: Namespace) -> None:

    ids = []

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
            exit(1)
        rm(tab="SHOP", value=ids, column=["shop_id"])
        print(f"removed {len(ids)} devices")
        exit(1)


# def remove_shop(dev: list[str], by: str, force: bool) -> None:
#     # remove device from shop based shop_id
#     rm(tab='SHOP', value=dev, column=['shop_id'])


def remove_dev(dev: list[str], by: str, force: bool) -> pd.DataFrame:
    # remove device based on hash or device_id
    # include all other tables where device is used
    # skip devices present in projects (unless forced)
    # return removed devices (not present in project)
    sql_scheme = read_json(SQL_SCHEME)

    # do not delete device if present in PROJECT table
    if not force:
        project_devs = getL(tab="PROJECT", get=[by], follow=True)
        dev = [d for d in dev if d not in project_devs]
    else:
        # unless you Forced to remove whole projects with device_id
        # NOT TESTED!!
        project_tab = getDF(tab="PROJECT", get=["proj_name", by], follow=True)
        project_tab = project_tab[project_tab[by].isin(dev)]
        project_tab = set(project_tab["proj_name"].tolist())
        rm(tab="PROJECT", value=project_tab, column=["proj_name"])
        rm(tab="PROJECT_INFO", value=project_tab, column=["project_name"])

    dev_id = getL(tab="DEVICE", get=["device_id"], search=dev, where=[by])

    all_tabs = list(sql_scheme.keys())
    # put DEVICE table on very end
    all_tabs.remove("DEVICE")
    all_tabs.append("DEVICE")
    for t in all_tabs:
        rm(tab=t, value=dev_id, column=["device_id"])
    return dev
