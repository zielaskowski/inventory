import os
from argparse import Namespace

import pandas as pd

from app.common import (
    BOM_HASH,
    BOM_PROJECT,
    BOM_QTY,
    DEV_DESC,
    DEV_HASH,
    DEV_ID,
    DEV_MAN,
    SHOP_DATE,
    SHOP_HASH,
    SHOP_ID,
    SHOP_PRICE,
    SHOP_QTY,
    SHOP_SHOP,
    STOCK_HASH,
    STOCK_QTY,
)
from app.message import MessageHandler
from app.sql import getDF
from app.tabs import prepare_project
from conf.config import DISP_CURR

msg = MessageHandler()


def trans(args: Namespace):
    """
    prepares transaction list for selected projects.
    Can split into available shops based on best price
    Will inform if device_id in shop but with different manufacturer
    """
    project = prepare_project(args.project)
    if not project:
        return
    # read BOM table from sql
    bom = getDF(
        tab="BOM",
        search=project,
        where=[BOM_PROJECT],
        follow=True,
    )

    # summarize on device_hash table
    agg_cols = {c: "first" for c in bom.columns if c != BOM_QTY}
    agg_cols.update({BOM_QTY: "sum"})
    bom = bom.groupby(DEV_HASH, as_index=False).agg(agg_cols)
    dev_list = bom[DEV_HASH].tolist()

    bom.loc[:, BOM_QTY] = bom.loc[:, BOM_QTY] * args.qty

    # read STOCK table from sql
    stock = getDF(tab="STOCK", search=dev_list, where=[STOCK_HASH])

    if not stock.empty:
        # merge BOM and STOCK on device_hash
        bom = bom.merge(stock, left_on=BOM_HASH, right_on=STOCK_HASH, how="left")
        bom[BOM_QTY] = bom[BOM_QTY] - bom[STOCK_QTY]
        bom = bom[bom[BOM_QTY] > 0]
        bom.drop(columns=[STOCK_QTY], inplace=True)

    # split BOM based on SHOP table data,
    # choose cheaper shop to export if device available in many
    # when data missing in SHOP tab, call it 'any'
    cart = getDF(tab="SHOP")
    cart["date"] = pd.to_datetime(cart["date"])
    cart = cart.merge(bom, left_on=SHOP_HASH, right_on=BOM_HASH, how="left")  # type: ignore
    # take only device_id from BOM
    cart = cart.loc[cart[SHOP_HASH].isin(dev_list)]
    # take only latest date
    cart = cart.loc[cart.groupby([SHOP_HASH, SHOP_SHOP])[SHOP_DATE].idxmax()]

    # if col 'order_qty' is greater then 'qty' then take 'order_qty'
    def qty_multiplication(row):
        """calculate multiplication SHOP_QTY based on required QTY"""
        qty = row[BOM_QTY]
        min_qty = row[SHOP_QTY]
        return ((qty + min_qty - 1) // min_qty) * min_qty

    if args.dont_split_shop:
        cart[SHOP_QTY] = cart[BOM_QTY]
    else:
        cart[SHOP_QTY] = cart.apply(qty_multiplication, axis="columns")
    cart[SHOP_PRICE] = cart[SHOP_QTY] * cart[SHOP_PRICE]
    # take minimum price
    cart = cart.loc[cart.groupby(SHOP_HASH)[SHOP_PRICE].idxmin()]

    bom.loc[:, SHOP_SHOP] = "any"
    bom.loc[:, SHOP_ID] = "-"
    bom = bom.merge(
        cart,
        left_on=BOM_HASH,
        right_on=STOCK_HASH,
        how="left",
        suffixes=("", "_2"),
    )
    if not args.dont_split_shop:
        bom[SHOP_SHOP] = bom[SHOP_SHOP + "_2"].combine_first(bom.loc[:, SHOP_SHOP])
        bom[SHOP_ID] = bom[SHOP_ID + "_2"].combine_first(bom.loc[:, SHOP_ID])
    cols = [
        c
        for c in [
            SHOP_QTY,
            DEV_ID,
            DEV_MAN,
            DEV_DESC,
            SHOP_SHOP,
            SHOP_ID,
        ]
        if c in bom.columns
    ]
    info = []
    if not args.dont_split_shop:
        for shop in bom[SHOP_SHOP].unique():
            file_csv = os.path.join(args.dir, args.file) + "_" + shop + ".csv"
            bom.loc[bom[SHOP_SHOP] == shop, cols].to_csv(file_csv, index=False)
            info += [
                {
                    "shop": shop,
                    "file": args.file,
                    "dir": args.dir,
                    "price": str(bom.loc[bom[SHOP_SHOP] == shop, SHOP_PRICE].sum())
                    + DISP_CURR,
                }
            ]

    else:
        file_csv = os.path.join(args.dir, args.file) + "_any.csv"
        bom[cols].to_csv(file_csv, index=False)
        info += [{"shop": None, "file": args.file, "dir": args.dir, "price": "-"}]
    msg.trans_summary(info)
