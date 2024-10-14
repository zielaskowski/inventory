from argparse import Namespace
from app.sql import getDF


def trans(args: Namespace):
    # read BOM table from sql
    BOM = getDF(tab="BOM", get=["qty", "device_hash"], follow=True)

    # summarize on device_hash table
    agg_cols = {c:'first' for c in BOM.columns if c != 'qty'}
    agg_cols['qty'] = 'sum'
    BOM = BOM.groupby("device_hash", as_index=False).agg(agg_cols)
    dev_list = BOM["device_hash"].tolist()

    # read STOCK table from sql
    STOCK = getDF(
        tab="STOCK", get=["%"], search=[dev_list], where=["device_hash"]
    )

    if not STOCK.empty:
        # merge BOM and STOCK on device_hash
        BOM = BOM.merge(STOCK, on="device_hash", how="left")
        BOM["qty"] = BOM["qty"] - BOM["stock_qty"]
        BOM = BOM[BOM["qty"] > 0]
        BOM.drop(columns=["stock_qty", "device_hash"], inplace=True)

    # split BOM based on SHOP table data,
    # choose cheaper shop to export if device available in many
    # when data missing in SHOP tab, call it 'any'

    BOM.drop(columns=["hash", "device_hash"], inplace=True)
    BOM.to_csv(args.file)
    print(f"{args.file} saved in {args.dir}")
