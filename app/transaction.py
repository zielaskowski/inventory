"""transaction functions"""

import os
import sys
from argparse import Namespace

import pandas as pd

import conf.config as conf
from app.message import msg
from app.sql import getDF
from app.tabs import prepare_project


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
        where=[conf.BOM_PROJECT],
        follow=True,
    )

    # summarize on device_hash table
    agg_cols = {c: "first" for c in bom.columns if c != conf.BOM_QTY}
    agg_cols.update({conf.BOM_QTY: "sum"})
    bom = bom.groupby(conf.DEV_HASH, as_index=False).agg(agg_cols)
    dev_list = bom[conf.DEV_HASH].tolist()

    bom.loc[:, conf.BOM_QTY] = bom.loc[:, conf.BOM_QTY] * args.qty

    # read STOCK table from sql
    stock = getDF(tab="STOCK", search=dev_list, where=[conf.STOCK_HASH])

    if not stock.empty:
        # merge BOM and STOCK on device_hash
        bom = bom.merge(
            stock, left_on=conf.BOM_HASH, right_on=conf.STOCK_HASH, how="left"
        )
        bom[conf.BOM_QTY] = bom[conf.BOM_QTY] - bom[conf.STOCK_QTY]
        bom = bom[bom[conf.BOM_QTY] > 0]
        bom.drop(columns=[conf.STOCK_QTY], inplace=True)
        if bom.empty:
            # all necessery devices in stock
            msg.msg("You have all devices in stock. Aborted.")
            sys.exit(0)

    # split BOM based on SHOP table data,
    # choose cheaper shop to export if device available in many
    # when data missing in SHOP tab, call it 'any'
    cart = getDF(tab="SHOP")
    cart["date"] = pd.to_datetime(cart["date"])
    cart = cart.merge(
        bom,  # type: ignore
        left_on=conf.SHOP_HASH,
        right_on=conf.BOM_HASH,
        how="left",
    )
    # take only device_id from BOM
    cart = cart.loc[cart[conf.SHOP_HASH].isin(dev_list)]
    # take only latest date
    cart = cart.loc[
        cart.groupby([conf.SHOP_HASH, conf.SHOP_SHOP])[conf.SHOP_DATE].idxmax()
    ]

    # if col 'order_qty' is greater then 'qty' then take 'order_qty'
    def qty_multiplication(row):
        """calculate multiplication SHOP_QTY based on required QTY"""
        qty = row[conf.BOM_QTY]
        min_qty = row[conf.SHOP_QTY]
        return ((qty + min_qty - 1) // min_qty) * min_qty

    if args.dont_split_shop:
        cart[conf.SHOP_QTY] = cart[conf.BOM_QTY]
    else:
        cart[conf.SHOP_QTY] = cart.apply(qty_multiplication, axis="columns")
    cart[conf.SHOP_PRICE] = cart[conf.SHOP_QTY] * cart[conf.SHOP_PRICE]
    # take minimum price
    cart = cart.loc[cart.groupby(conf.SHOP_HASH)[conf.SHOP_PRICE].idxmin()]

    bom.loc[:, conf.SHOP_SHOP] = "any"
    bom.loc[:, conf.SHOP_ID] = "-"
    bom = bom.merge(
        cart,
        left_on=conf.BOM_HASH,
        right_on=conf.STOCK_HASH,
        how="left",
        suffixes=("", "_2"),
    )
    if not args.dont_split_shop:
        bom[conf.SHOP_SHOP] = bom[conf.SHOP_SHOP + "_2"].combine_first(
            bom.loc[:, conf.SHOP_SHOP]
        )
        bom[conf.SHOP_ID] = bom[conf.SHOP_ID + "_2"].combine_first(
            bom.loc[:, conf.SHOP_ID]
        )
    cols = [
        c
        for c in [
            conf.SHOP_QTY,
            conf.DEV_ID,
            conf.DEV_MAN,
            conf.DEV_DESC,
            conf.SHOP_SHOP,
            conf.SHOP_ID,
        ]
        if c in bom.columns
    ]
    info = []
    if not args.dont_split_shop:
        for shop in bom[conf.SHOP_SHOP].unique():
            file_csv = os.path.join(args.dir, args.file) + "_" + shop + ".csv"
            bom.loc[bom[conf.SHOP_SHOP] == shop, cols].to_csv(file_csv, index=False)
            info += [
                {
                    "shop": shop,
                    "file": args.file,
                    "dir": args.dir,
                    "price": str(
                        bom.loc[bom[conf.SHOP_SHOP] == shop, conf.SHOP_PRICE].sum()
                    )
                    + conf.DISP_CURR,
                }
            ]

    else:
        file_csv = os.path.join(args.dir, args.file) + "_any.csv"
        bom[cols].to_csv(file_csv, index=False)
        info += [{"shop": None, "file": args.file, "dir": args.dir, "price": "-"}]
    msg.trans_summary(info)
