"""when importing with alternate manufacturer, whatever device_id:
- ask during import
- adm -a: do not work

when importing already existing device with different manufacturer:
- ask during import
- adm -a: will ask and modify alternatives: see below
add new manufacturers to alternatives and present all options
Alternatives are presented in two columns and user can select
one of it or give something else
man_in the db | man_opt possible alternatives
user selection: left, right or new
1. user leaving what is:
  - do nothing
2. user selecting one of presented alternatives
  - modify device, do not change alternatives
3. user write own manufacturer
  - modify device, add new manufacturer and alternative to db
CHECK
if any manufacturer is in alternative list, move it to alternatives
"""

import pandas as pd

import conf.config as conf
from app import sql
from app.error import ReadJsonError
from app.message import msg


def write_man_alternatives(man_alt: dict) -> None:
    """store manufacturers into tables"""
    if [k for k in man_alt if k] == []:
        # nothing to do
        msg.msg("empty file")
        raise ReadJsonError(conf.MAN_ALT, type_val="List")
    man = pd.DataFrame(columns=[conf.MAN_NAME, conf.MAN_ALT_NAME])  # pyright: ignore
    for k, val in man_alt.items():
        if not k:
            continue
        for v in val:
            new_row = pd.DataFrame(
                {conf.MAN_NAME: k, conf.MAN_ALT_NAME: v}, index=[0]  # pyright: ignore
            )
            man = pd.concat([man, new_row], ignore_index=True)
    sql.put(
        dat=pd.DataFrame({conf.MAN_NAME: man[conf.MAN_NAME].unique()}),
        tab="MANUFACTURER",
    )
    base_man = sql.getDF(
        tab="MANUFACTURER",
        search=list(man_alt.keys()),
        where=[conf.MAN_NAME],
    )
    alt_man = pd.merge(left=man, right=base_man, how="left", on=conf.MAN_NAME)
    sql.put(dat=alt_man, tab="ALTERNATIVE_MANUFACTURER")


def get_man_alternatives() -> dict:
    """get manufacturers from db"""
    alt_man = {}
    alt_man_df = sql.getDF("ALTERNATIVE_MANUFACTURER", follow=True)
    if alt_man_df.empty:
        return alt_man
    for m in alt_man_df[conf.MAN_NAME].unique():
        mask = alt_man_df[conf.MAN_NAME] == m
        alt_man[m] = alt_man_df.loc[mask, conf.MAN_ALT_NAME].to_list()
    return alt_man


def store_alternatives(
    alternatives: dict[str, list[str]],
    selection: list[str],
) -> None:
    """
    write selection made by user, so next time no need to choose
    only for DEV_MAN column and only if selection was one-to-one
    """
    if alternatives == {}:
        return
    alt_keys = list(alternatives.keys())
    alt_len = len(alternatives[alt_keys[0]])
    alt_from = alternatives[alt_keys[0]]
    try:
        alt_exist = get_man_alternatives()
    except ReadJsonError as e:
        msg.msg(str(e))
        return

    for i in range(alt_len):
        # only one-to-one alternatives and changed
        if alt_from[i] != selection[i]:
            # remove alternative if already exists
            for k in list(alt_exist.keys()):
                if alt_from[i] in alt_exist[k]:
                    alt_exist[k].remove(alt_from[i])
                    if alt_exist[k] == []:
                        alt_exist.pop(k)
            if selection[i] in alt_exist:
                alt_exist[selection[i]].append(alt_from[i])
                alt_exist[selection[i]] = list(set(alt_exist[selection[i]]))
            else:
                alt_exist[selection[i]] = [alt_from[i]]
    write_man_alternatives(alt_exist)


def get_alternatives(manufacturers: list[str]) -> tuple[list[str], list[bool]]:
    """
    check if we have match from stored alternative
    return list with replaced manufacturers
    (complete list, including not replaced also)
    and list of bools indicating where replaced
    """
    alt_exist = get_man_alternatives()
    man_replaced = []
    for man in manufacturers:
        rep = [k for k, v in alt_exist.items() if man in v]
        if rep != []:
            man_replaced.append(rep[0])
        else:
            man_replaced.append(man)
    # inform user about alternatives (be explicit!)
    alt = pd.DataFrame({"was": manufacturers, "alternative": man_replaced})
    differ_row = alt["was"] != alt["alternative"]
    if not alt.loc[differ_row, :].empty:
        if not msg.inform_alternatives(alternatives=alt.loc[differ_row, :]):
            return manufacturers, []
    return man_replaced, differ_row.to_list()
