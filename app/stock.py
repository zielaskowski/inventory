"""
Function to handle stock: commiting, displaying
"""

from argparse import Namespace

from app import sql
from app.common import BOM_PROJECT, BOM_QTY, STOCK_QTY
from app.message import MessageHandler
from app.tabs import prepare_project

msg = MessageHandler()


def stock(args: Namespace) -> None:
    """Main stock function"""
    if args.project:
        commit_project(args=args)
        return


def commit_project(args: Namespace) -> None:
    """commit projects"""
    if (projects := prepare_project(projects=args.project, commited=False)) == []:
        return
    dat = sql.getDF(
        tab="BOM",
        search=projects,
        where=[BOM_PROJECT],
    )
    dat.rename(columns={BOM_QTY: STOCK_QTY}, inplace=True)
    dat[STOCK_QTY] = dat[STOCK_QTY] * args.qty
    sql.put(dat, tab="STOCK")
    msg.bom_commit(projects)
