"""
Main functions:
    align_other_cols(): aligning different columns on the same dev_hash,
    use_alt_man(): applying alternative manufacturers,
    find_alt_man(): finding alternative manufacturer when the same dev_id
and few helpers

When importing with alternate manufacturer:
    - just use alternative manufacturers stored: use_alt_man()
    - align other columns for the same dev_hash: align_other_cols()
    - check different manufacturers on the same device_id: find_alt_man(just_inform)

When called from admin, through tabs.align_data():
importing already existing device with different manufacturer:
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
from app.common import first_diff_index, tab_cols
from app.error import ReadJsonError, VimdiffSelError
from app.message import msg
from app.vimdiff import vimdiff_selection


def write_alt_man(man_alt: dict) -> None:
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


def get_alt_man() -> dict:
    """get manufacturers from db"""
    alt_man = {}
    alt_man_df = sql.getDF("ALTERNATIVE_MANUFACTURER", follow=True)
    if alt_man_df.empty:
        return alt_man
    for m in alt_man_df[conf.MAN_NAME].unique():
        mask = alt_man_df[conf.MAN_NAME] == m
        alt_man[m] = alt_man_df.loc[mask, conf.MAN_ALT_NAME].to_list()
    return alt_man


def store_alt_man(
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
        alt_exist = get_alt_man()
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
    write_alt_man(alt_exist)


def use_alt_man(manufacturers: list[str]) -> tuple[list[str], list[bool]]:
    """
    check if we have match from stored alternative
    return list with replaced manufacturers
    (complete list, including not replaced also)
    and list of bools indicating where replaced
    """
    alt_exist = get_alt_man()
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


def find_alt_man(  # pylint: disable=too-many-return-statements
    dat: pd.DataFrame,
    just_inform: bool = False,
) -> pd.DataFrame:
    """
    For device_id duplication, collect manufacturer name if different
    create dictionary with list of manufacturers as values for device_id as key:
    {DEV_ID:[MAN1,MAN2,...]}
    1.  checking only incoming data against stock. Do not check duplication
        inside incoming data
    2.  simplest case is one-to-one or many-to-one: only one device in stock
    3.  tricky is when are many devices in stock: present manufacturers separated with
        '|' as list. Special macro in vim to pick option
    return DataFrame modified by user
    when 'just_inform', just display messages about possible duplication
    and ref to admin funcs
    """
    start_line = 1  # start line for vim (vim count from 1)
    if not all(c in dat.columns for c in [conf.DEV_ID, conf.DEV_MAN]):
        return dat
    ex_dat = sql.getDF(
        tab="DEVICE",
        get_col=[conf.DEV_ID, conf.DEV_MAN],
        search=dat[conf.DEV_ID].to_list(),
        where=[conf.DEV_ID],
    )
    if ex_dat.empty:
        # no duplication in stock
        return dat

    # group existing data on dev, join manufacturers with ' | ' if more then one dev
    ex_grp = (
        ex_dat.groupby(conf.DEV_ID)[conf.DEV_MAN]
        .agg(lambda x: " | ".join(x.astype(str)))
        .reset_index()
    )

    # iterate as long as there is a change from user
    while True:
        # panda RULES: must be merged on 'left' to keep indexes, and clean up NAs later
        # with merge on 'inner' indexed are fuck up
        dat_dup = pd.merge(
            left=dat,
            right=ex_grp,
            on=conf.DEV_ID,
            how="left",
            suffixes=("", "_opts"),
        )

        # column name for grouped manufacturers
        man_grp_col = conf.DEV_MAN + "_opts"

        # for GREAT panda NaN is any value so must be droped before
        dat_dup = dat_dup.loc[~dat_dup[man_grp_col].isna(), :]

        # drop rows with matched manufacturer
        dat_dup = dat_dup[dat_dup[conf.DEV_MAN] != dat_dup[man_grp_col]]

        if dat_dup.empty:
            # importing manufacturers are aligned with stock
            return dat

        # remove dup_col from dup_col_grp
        for r in dat_dup.itertuples():
            dup_col_grp_v = getattr(r, man_grp_col)
            dup_col_v = getattr(r, conf.DEV_MAN)
            opts = str(dup_col_grp_v).split(" | ")
            opts = [o for o in opts if o != dup_col_v]
            dat_dup.loc[r.Index, man_grp_col] = " | ".join(opts)

        if just_inform:
            msg.inform_duplications(
                dup=dat_dup.loc[:, [conf.DEV_MAN, man_grp_col, conf.DEV_ID]]
            )
            return dat
        # sort data on device_id, much easier to understand in vimdiff
        dat_dup.sort_values(by=conf.DEV_ID, inplace=True)
        chosen, alternatives = vimdiff_selection(
            ref_col={"devices": dat_dup.loc[:, conf.DEV_ID].to_list()},
            change_col={conf.DEV_MAN: dat_dup.loc[:, conf.DEV_MAN].to_list()},
            opt_col={man_grp_col: dat_dup.loc[:, man_grp_col].to_list()},
            what_differ=conf.DEV_MAN,
            dev_id="",
            exit_on_change=True,
            start_line=start_line,
        )
        # write matches selected by user, so next time save some time
        store_alt_man(
            alternatives=alternatives,
            selection=chosen,
        )

        # if no change from user, finish
        if dat_dup[conf.DEV_MAN].to_list() == chosen:
            break
        # find index of change, so can start vim on correct line number
        start_line = first_diff_index(dat_dup[conf.DEV_MAN].to_list(), chosen)
        dat_dup[conf.DEV_MAN] = chosen
        # put new data into original tab, based on preserved indexes
        dat.loc[dat_dup.index, conf.DEV_MAN] = dat_dup[conf.DEV_MAN]

    return dat


def align_other_cols(  # pylint: disable=too-many-locals
    rm_dat: pd.DataFrame,
    keep_dat: pd.DataFrame,
) -> pd.DataFrame:
    """
    expect DataFrame with one pair replacement per row:
    and columns:
        rm_dat: dev_rm | man_rm | DEV_HASH | DEV_MAN + must_cols
        keep_dat: must_cols+nice_cols
    display all "nice" attributes of device to user to choose
    display only not empty and attributes that differ
    if keep_attr empty, replace with rm_attr
    return selected attributes attached in row to input dat
    raise KeyboardInterrupt when user abort
    """
    if rm_dat.empty or keep_dat.empty:
        raise ValueError("Input DataFrame can not be empty.")

    extra_cols = ["dev_rm", "man_rm"]
    must_cols, nice_cols = tab_cols(tab="DEVICE")

    missing_rm_cols = [c for c in must_cols + extra_cols if c not in rm_dat.columns]
    if any(missing_rm_cols):
        raise ValueError(f"Missing necessery columns in rm_dat: {missing_rm_cols}")
    missing_keep_cols = [c for c in must_cols + nice_cols if c not in keep_dat.columns]
    if any(missing_keep_cols):
        raise ValueError(f"Missing necessery columns in keep_dat: {missing_keep_cols}")

    keep_dat.set_index(conf.DEV_HASH, inplace=True)
    # collect all useful data from devices we are about to remove
    for idx in rm_dat.index:
        rm_attr = rm_dat.copy(deep=True).loc[idx, :]
        # collect attributes for devices that we want to use
        keep_attr = keep_dat.loc[keep_dat.index == rm_attr[conf.DEV_HASH]].iloc[0, :]
        change_man = keep_attr[conf.DEV_MAN]
        opt_man = rm_attr["man_rm"]
        ref_id = rm_attr[conf.DEV_ID]
        rm_attr, keep_attr, keep_nones = align_attributes(rm_attr, keep_attr)
        if not keep_attr.empty or not keep_nones.empty:
            try:
                aligned_dat, _ = vimdiff_selection(
                    ref_col={"columns": rm_attr.index.to_list()},
                    change_col={change_man: keep_attr.to_list()},
                    opt_col={opt_man: rm_attr.to_list()},
                    what_differ="attributes",
                    dev_id=ref_id,
                    exit_on_change=False,
                )
            except VimdiffSelError as err:
                # if no change from user, skip
                print(str(err))
                if err.interact:
                    # raised when user mess up with lines order or number
                    input("Press any key....")
                continue
            keep_attr.iloc[:] = aligned_dat
            # in case keep_attr is missing indexes
            keep_attr = keep_attr.reindex(keep_attr.index.union(keep_nones.index))
            keep_attr.update(keep_nones)
            # update will put NaN in missing cols, which will brake on int columns
            # so update on common cols only
            keep_attr_df = pd.DataFrame([keep_attr])
            common_col = keep_dat.columns.intersection(keep_attr_df.columns)
            keep_dat.loc[keep_attr_df.index, common_col] = keep_attr_df
    return keep_dat.reset_index()


def align_attributes(rm_attr: pd.Series, keep_attr: pd.Series) -> tuple:
    """
    Align attributes of devices:
        rm_attr: this attributes will lost if not aligned
        keep_attr: this we optionally may want to keep
    """
    # if NA in rm_attr or missing col, take from keep_attr if present
    for idx, val in keep_attr.items():
        if idx not in rm_attr:
            rm_attr[idx] = val
        if rm_attr[idx] is None:
            rm_attr[idx] = keep_attr[idx]
    # and opposite, then will be easy to remove the same
    for idx, val in rm_attr.items():
        if idx not in keep_attr:
            keep_attr[idx] = val
    # hide attributes which are already aligned
    for idx in rm_attr.index:
        rm, keep = rm_attr[idx], keep_attr[idx]
        # IEEE754: NaN is not equal to anything, including itself!
        if (rm == keep) or (pd.isna(rm) & pd.isna(keep)):
            rm_attr.pop(idx)
            keep_attr.pop(idx)
    # if None in keep_dat and not in rm_dat, use and write separate Series
    index_none = rm_attr.loc[keep_attr.isna() & rm_attr.notna()].index
    keep_none = rm_attr[index_none]
    rm_attr.drop(index_none, inplace=True, errors="ignore")
    keep_attr.drop(index_none, inplace=True, errors="ignore")
    return (
        rm_attr.sort_index(),
        keep_attr.sort_index(),
        keep_none,
    )
