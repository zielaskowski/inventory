import os
import pandas as pd
from argparse import Namespace

from app.sql import getDF
from app.error import messageHandler

msg = messageHandler()


def trans(args: Namespace):
    # read BOM table from sql
    BOM = getDF(tab="BOM", follow=True)

    # summarize on device_hash table
    agg_cols = {c: "first" for c in BOM.columns if c != "qty"}
    agg_cols.update({"qty": "sum"})
    BOM = BOM.groupby("device_id", as_index=False).agg(agg_cols)
    dev_list = BOM["device_id"].tolist()

    BOM["qty"] = BOM["qty"] * args.qty

    # read STOCK table from sql
    STOCK = getDF(tab="STOCK", search=[dev_list], where=["device_id"])

    if not STOCK.empty:
        # merge BOM and STOCK on device_hash
        BOM = BOM.merge(STOCK, on="device_id", how="left")
        BOM["qty"] = BOM["qty"] - BOM["stock_qty"]
        BOM = BOM[BOM["qty"] > 0]
        BOM.drop(columns=["stock_qty"], inplace=True)

    # split BOM based on SHOP table data,
    # choose cheaper shop to export if device available in many
    # when data missing in SHOP tab, call it 'any'
    cart = getDF(tab="SHOP")
    cart["date"] = pd.to_datetime(cart["date"])
    cart = cart.merge(BOM, on="device_id", how="left")
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

    BOM["shop"] = "any"
    BOM["shop_id"] = "-"
    BOM = BOM.merge(cart, on="device_id", how="left", suffixes=("", "_2"))
    BOM["shop"] = BOM["shop" + "_2"].combine_first(BOM["shop"])
    BOM["shop_id"] = BOM["shop_id" + "_2"].combine_first(BOM["shop_id"])
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
        if c in BOM.columns
    ]
    info = []
    if not args.dont_split_shop:
        for shop in BOM["shop"].unique():
            file_csv = os.path.join(args.dir, args.file) + "_" + shop + ".csv"
            BOM.loc[BOM["shop"] == shop, cols].to_csv(file_csv, index=False)
            info += [
                {'shop':shop,
                 'file':args.file,
                 'dir':args.dir,
                 'price': BOM.loc[BOM["shop"] == shop, 'shop_price'].sum()
                 }
            ]

    else:
        file_csv = os.path.join(args.dir, args.file) + ".csv"
        BOM[cols].to_csv(file_csv, index=False)
        info += [
                {'shop':None,
                 'file':args.file,
                 'dir':args.dir
                 }
            ]
    msg.trans_summary(info)