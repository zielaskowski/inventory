"""
Logging class. Log only commands changing the db.
Logs are kept in sql db in LOG table: id|date|command
Intention is to visualize user what to undo
Methods:
    - log - store arguments for logging
    - log_read - read what stored in db
    - low_write - write arguments to db

"""

import sys
from argparse import Namespace
from datetime import datetime

import pandas as pd

import conf.config as conf
from app import sql
from app.common import int_to_date_log
from app.message import msg


class Log:
    """logging methods"""

    def __init__(self) -> None:
        # log only once, each logging by sql_execute will change to FALSE
        # one command usually trigger multiple sql operation
        self.log_on = True
        self.cmd = []

    def log(self, args: Namespace) -> None:
        """
        store log for later write
        """

        self.cmd = ["python -m inv"]
        for var, val in vars(args).items():
            if callable(val):
                continue
            if val:
                if var == "command":
                    self.cmd += [val]
                else:
                    if isinstance(val, list):
                        val = " ".join(f"'{v}'" for v in val)
                    self.cmd += [
                        "--"
                        + var
                        + " "
                        + (str(val) if not isinstance(val, bool) else "")
                    ]

    def log_read(self, n: int) -> pd.DataFrame:
        """
        read form log
        can rise IsDirectoryError and FileNotFoundError
        split dates from commands and return as tuple
        """
        try:
            logs = sql.getDF(tab="LOG")
            if logs.empty:
                return logs
        except (sql.SqlGetError, sql.SqlExecuteError) as e:
            msg.msg(str(e))
            sys.exit(1)
        n = min(n, len(logs))
        logs = logs.loc[len(logs) - n : len(logs), :]
        # reverse index so easier to select
        logs.sort_values(by=conf.LOG_DATE, inplace=True, ascending=False)
        logs.reset_index(inplace=True, drop=True)
        logs.sort_values(by=conf.LOG_DATE, inplace=True)
        logs.reset_index(inplace=True)
        logs["id"] = logs["index"].apply(lambda x: x + 1)
        logs["date_fmt"] = logs["date"].apply(int_to_date_log)
        return logs

    def log_write(self, force=False) -> None:
        """
        write to log only once (unless force==True)
        can rise IsDirectoryError and FileNotFoundError
        """
        if not self.log_on and not force:
            return
        self.log_on = False
        now = datetime.now()
        now = now.strftime("%s")
        try:
            sql.put(
                tab="LOG",
                dat=pd.DataFrame(
                    {conf.LOG_DATE: now, conf.LOG_ARGS: " ".join(self.cmd)},
                    index=[1],  # pyright: ignore
                ),
            )
        except (sql.SqlExecuteError, sql.SqlTabError) as e:
            msg.msg(str(e))
            sys.exit(1)


log = Log()
