from argparse import Namespace
import pandas as pd

from app.sql import rm, getDF, getL, put
from app.tabs import tab_cols, align_manufacturer
from conf.config import SQL_scheme
from app.common import read_json, print_file, hash_table


def admin(args: Namespace) -> None:
    sql_scheme = read_json(SQL_scheme)

    if args.config:
        print_file('./conf/config.py')

    if args.remove_dev_id:
        dev = remove_dev(args.remove_dev_id, "device_id", args.force)
        print(f"removed {len(dev)} devices")
        exit(1)

    if args.remove_hash_id:
        dev = remove_dev(args.remove_hash_id, "hash", args.force)
        print(f"removed {len(dev)} devices")
        exit(1)

    if args.align_manufacturer:
        # clear BOM
        # take all data from DEVICES table
        dev = getDF(tab="DEVICE",follow=True)
        # group by device_id, select only groups with more then one row 
        # and take the first
        dev_double = (dev.
                      groupby("device_id").
                      filter(lambda x: len(x) > 1).
                      groupby("device_id").
                      nth(1))
        if dev_double.empty:
            print("No duplicate device_id found")
            exit(1)
        
        # remove dev_double from dev so to not overlap
        dev = dev[~dev["hash"].isin(dev_double["hash"])]
        remove_dev(dev=dev_double["hash"].tolist(), 
                    by="hash", 
                    force=False)

        dev_double = align_manufacturer(dev_double, dev)
        
        # write devices with merged manufacturer and longer description
        # hash again
        dev_double['hash'] = (dev_double.
                              apply(lambda x: hash_table(
                                  x,
                                  ['device_id','device_manufacturer']
                                  ), 
                                  axis=1))
        put(dat=dev_double,
            tab='DEVICE',
            on_conflict=sql_scheme['DEVICE']["ON_CONFLICT"])
        exit(1)


def remove_dev(dev: list[str], by: str, force: bool) -> pd.DataFrame:
    # remove device based on hash or device_id
    # include all other tables where device is used
    # skip devices present in projects (unless forced)
    # return removed devices (not present in project)
    sql_scheme = read_json(SQL_scheme)

    # do not delete device if present in PROJECT table
    if not force:
        project_devs = getL(tab="PROJECT", get=[by], follow=True)
        dev = [d for d in dev if d not in project_devs]
    else:
    # unless you Forced to remove whole projects with device_id
    # NOT TESTED!!
        project_tab = getDF(
            tab="PROJECT", get=["proj_name", by], follow=True
        )
        project_tab = project_tab[project_tab[by].isin(dev)]
        project_tab = set(project_tab["proj_name"].tolist())
        rm(tab="PROJECT", value=project_tab, column=["proj_name"])
        rm(tab="PROJECT_INFO", value=project_tab, column=["project_name"])

    hash_id = getL(tab="DEVICE", get=["hash"], search=dev, where=[by])

    all_tabs = list(sql_scheme.keys())
    # put DEVICE table on very end
    all_tabs.remove("DEVICE")
    all_tabs.append("DEVICE")
    for t in all_tabs:
        _, _, hash_cols = tab_cols(t)
        hashed_col = [
            k
            for k, v in hash_cols.items()
            if "device_id" in v and k in sql_scheme[t]
        ]
        rm(tab=t, value=hash_id, column=hashed_col)
    return dev