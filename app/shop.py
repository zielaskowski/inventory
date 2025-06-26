from argparse import Namespace

from app.bom import import_file
from app.message import MessageHandler
from app.tabs import (
    bom_info,
    bom_template,
    import_tab,
    scan_files,
)

msg = MessageHandler()


def shop_import(args: Namespace) -> None:
    """import shopping cart from file"""
    if args.info:
        bom_info(tab="SHOP")
        return
    if args.csv_template:
        bom_template(args=args, tab="SHOP")
        return

    files = scan_files(args)
    for file in files:
        new_stock = import_file(args, file)
        if new_stock.empty:
            continue
        import_tab(
            dat=new_stock,
            tab="SHOP",
            args=args,
            file=file,
        )
