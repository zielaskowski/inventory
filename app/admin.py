from argparse import Namespace

from app.sql import rm, getDF, getL
from app.tabs import __tab_cols__
from conf.config import SQL_scheme
from app.common import read_json


def admin(args: Namespace) -> None:
    if args.remove_dev_id:
        remove_dev(args.remove_dev_id, "device_id", args.force)
        exit(1)
    if args.remove_hash_id:
        remove_dev(args.remove_hash_id, "hash", args.force)
        exit(1)


def remove_dev(dev: list[str], by: str, force: bool) -> None:
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
        _, _, hash_cols = __tab_cols__(t)
        hashed_col = [
            k
            for k, v in hash_cols.items()
            if "device_id" in v and k in sql_scheme[t]
        ]
        rm(tab=t, value=hash_id, column=hashed_col)
