import os
from argparse import Namespace

import pandas as pd

from app.common import BOM_PROJECT
from app.message import messageHandler
from app.sql import getDF
from app.tabs import prepare_project

msg = messageHandler()


def trans(args: Namespace):
    """
    prepares transaction list for selected projects.
    Can split into available shops based on best price
    Will inform if device_id in shop but with different manufacturer
    """
    project = prepare_project(args.project, commited=False)
    # read BOM table from sql
    bom = getDF(
        tab="BOM",
        search=project,
        where=[BOM_PROJECT],
        follow=True,
    )

    # summarize on device_hash table
    agg_cols = {c: "first" for c in bom.columns if c != "qty"}
    agg_cols.update({"qty": "sum"})
    bom = bom.groupby("device_id", as_index=False).agg(agg_cols)
    dev_list = bom["device_id"].tolist()

    bom.loc[:, "qty"] = bom.loc[:, "qty"] * args.qty

    # read STOCK table from sql
    stock = getDF(tab="STOCK", search=[dev_list], where=["device_id"])

    if not stock.empty:
        # merge BOM and STOCK on device_hash
        bom = bom.merge(stock, on="device_id", how="left")
        bom["qty"] = bom["qty"] - bom["stock_qty"]
        bom = bom[bom["qty"] > 0]
        bom.drop(columns=["stock_qty"], inplace=True)

    # split BOM based on SHOP table data,
    # choose cheaper shop to export if device available in many
    # when data missing in SHOP tab, call it 'any'
    cart = getDF(tab="SHOP")
    cart["date"] = pd.to_datetime(cart["date"])
    cart = cart.merge(bom, on="device_id", how="left")
    # take only device_id from BOM
    cart = cart.loc[cart["device_id"].isin(dev_list)]
    # take only latest date
    cart = cart.loc[cart.groupby(["device_id", "shop"])["date"].idxmax()]
    # if col 'order_qty' is greater then 'qty' then take 'order_qty'
    cart["shop_qty"] = cart.apply(
        lambda x: x["order_qty"] if x["order_qty"] > x["qty"] else x["qty"],
        axis="columns",
    )
    cart["shop_price"] = cart["shop_qty"] * cart["price"]
    # take minimum price
    cart = cart.loc[cart.groupby("device_id")["shop_price"].idxmin()]

    bom.loc[:, "shop"] = "any"
    bom.loc[:, "shop_id"] = "-"
    bom = bom.merge(cart, on="device_id", how="left", suffixes=("", "_2"))
    bom["shop"] = bom["shop" + "_2"].combine_first(bom.loc[:, "shop"])
    bom["shop_id"] = bom["shop_id" + "_2"].combine_first(bom.loc[:, "shop_id"])
    cols = [
        c
        for c in [
            "qty",
            "device_id",
            "device_manufacturer",
            "device_description",
            "shop",
            "shop_id",
        ]
        if c in bom.columns
    ]
    info = []
    if not args.dont_split_shop:
        for shop in bom["shop"].unique():
            file_csv = os.path.join(args.dir, args.file) + "_" + shop + ".csv"
            bom.loc[bom["shop"] == shop, cols].to_csv(file_csv, index=False)
            info += [
                {
                    "shop": shop,
                    "file": args.file,
                    "dir": args.dir,
                    "price": bom.loc[bom["shop"] == shop, "shop_price"].sum(),
                }
            ]

    else:
        file_csv = os.path.join(args.dir, args.file) + ".csv"
        bom[cols].to_csv(file_csv, index=False)
        info += [{"shop": None, "file": args.file, "dir": args.dir}]
    msg.trans_summary(info)
