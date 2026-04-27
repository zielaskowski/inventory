"""
Main functions:
    vimdiff_selection - present three columns to user to make selection
    vimdiff_config  - configure vim to be "more user friendly

use two files from conf directory:
    - vimdiff_help.txt - text file with sage help to be presented to user
    - .vimrc - vim configuration with macros, display arrangement and colors
"""

import os
import re
import subprocess
from typing import Dict, List, Tuple

import pandas as pd
from jinja2 import Template

import conf.config as conf
from app.error import VimdiffSelError


def vimdiff_selection(  # pylint: disable=too-many-positional-arguments,too-many-locals,too-many-arguments
    ref_col: dict[str, list[str]],
    change_col: dict[str, list[str]],
    opt_col: dict[str, list[str]],
    what_differ: str,
    dev_id: str,
    exit_on_change: bool,
    start_line: int = 1,
) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    present alternatives in vimdiff
    input is dict, where key is column_name and values are to be displayed
    column_name is also used to name the file where values are written and then displayed by vimdiff
    """
    cols = []
    for col in [ref_col, change_col, opt_col]:
        cols.append(next(iter(col)))
        cols.append(next(iter(col.values())))
    ref_k, ref_v = (0, 1)
    change_k, change_v = (2, 3)
    opt_k, opt_v = (4, 5)
    # if empty list (nothing to write, nothing to show)
    # just return
    if cols[change_v] == [] or cols[opt_v] == []:
        return [], {}

    for key in [ref_k, change_k, opt_k]:
        # remove forbidden chars: / \ : * ? " < > |
        cols[key] = re.sub(r'[\/\\:\*\?"<>\|\r\n\t]', "_", cols[key])
        with open(
            # in case manufacturers are the same, need to add suffix to file name
            conf.TEMP_DIR + cols[key] + "_" + str(key) + ".txt",
            mode="w",
            encoding="UTF8",
        ) as f:
            for ind, item in enumerate(cols[key + 1]):
                f.write(str(ind) + "| " + str(item) + "\n")  # add line number: '1| txt'
                # other way diff may shift rows to align data between columns

    vimdiff_config(
        ref_col=cols[ref_k] + "_0",
        change_col=cols[change_k] + "_2",
        opt_col=cols[opt_k] + "_4",
        what_differ=what_differ,
        dev_id=dev_id,
        exit_on_change=exit_on_change,
        start_line=start_line,
    )

    vim_cmd = "vim -u " + conf.TEMP_DIR + ".vimrc"
    if conf.DEBUG == "none":
        subprocess.run(vim_cmd, shell=True, check=False)
    elif conf.DEBUG == "debugpy":
        with subprocess.Popen("konsole -e " + vim_cmd, shell=True) as p:
            p.wait()

    with open(
        conf.TEMP_DIR + cols[change_k] + "_2.txt", mode="r", encoding="UTF8"
    ) as f:
        chosen = f.read().splitlines()

    # clean up files
    for key in [ref_k, change_k, opt_k]:
        os.remove(conf.TEMP_DIR + cols[key] + "_" + str(key) + ".txt")

    if chosen == []:  # user interrupt
        raise VimdiffSelError(user_inerrupt=True)

    # remove line numbers
    chosen = [re.sub(r"^\d+\|\s*", "", c) for c in chosen]
    chosen = [c.strip() for c in chosen]
    if any(len(chosen) != len(cols[v]) for v in [ref_v, change_v, opt_v]):
        # if user mess-up or added/removed rows
        max_len = max(len(chosen), len(cols[opt_v]))
        chosen += [None] * (max_len - len(chosen))
        cols[opt_v] += [None] * (max_len - len(cols[opt_v]))
        df = pd.DataFrame({"selected": chosen, "opts": cols[opt_v]})
        raise VimdiffSelError(select=df, interact=conf.DEV_MAN != cols[change_k])

    alternatives = {
        cols[change_k]: cols[change_v],
        cols[opt_k]: cols[opt_v],
    }
    return chosen, alternatives


def vimdiff_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ref_col: str,
    change_col: str,
    opt_col: str,
    what_differ: str,
    dev_id: str,
    exit_on_change: bool,
    start_line: int = 1,
):
    """
    prepare vimdiff config from template
    and adjust help message to column
    ref_col, change_col, opt_col are files (without extension) which
                                 will be displayed
    alternate_col is the name of column, displayed in help, when
                  equal DEV_MAN will turn on option selection help and options
    dev_id, used during attributes alignment, to inform about device
    exit_on_change if True, will exit vim after each change
                   (to update file context for exmple)
    """
    with open(
        os.path.join(conf.MODULE_PATH, "conf", "vimdiff_help.txt"),
        mode="r",
        encoding="UTF8",
    ) as f:
        help_temp = Template(f.read())
    with open(
        os.path.join(conf.MODULE_PATH, "conf", ".vimrc"),
        mode="r",
        encoding="UTF8",
    ) as f:
        vimrc_temp = Template(f.read())

    substitutions = {
        "START_LINE": start_line,
        "TEMP_DIR": conf.TEMP_DIR,
        "LEFT_NAME": opt_col,
        "RIGHT_NAME": change_col,
        "WHAT_DIFFER": what_differ,
        "DEV_ID": dev_id,
        "REF_COL": ref_col,
        "MULTIPLE_MANUFACTURERS": conf.DEV_MAN == what_differ,
        "EXIT_ON_CHANGE": exit_on_change,
    }
    vimrc_txt = vimrc_temp.render(substitutions)
    help_txt = help_temp.render(substitutions)
    with open(os.path.join(conf.TEMP_DIR, ".vimrc"), mode="w", encoding="UTF8") as f:
        f.write(vimrc_txt)
    with open(
        os.path.join(conf.TEMP_DIR, "vimdiff_help.txt"), "w", encoding="UTF8"
    ) as f:
        f.write(help_txt)
