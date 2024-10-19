from argparse import Namespace
import pandas as pd

from app.sql import rm, getDF, getL
from conf.config import SQL_scheme
from app.common import read_json, print_file


def admin(args: Namespace) -> None:

    if args.config:
        print_file('./conf/config.py')

    if args.remove_dev_id:
        dev = remove_dev(args.remove_dev_id, "device_id", args.force)
        print(f"removed {len(dev)} devices")
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

    dev_id = getL(tab="DEVICE", get=["device_id"], search=dev, where=[by])

    all_tabs = list(sql_scheme.keys())
    # put DEVICE table on very end
    all_tabs.remove("DEVICE")
    all_tabs.append("DEVICE")
    for t in all_tabs:
        rm(tab=t, value=dev_id, column=['device_id'])
    return dev