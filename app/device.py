import pandas as pd

from app.sql import getDF


DEV_ID = "device_id"
DEV_MAN = "device_manufacturer"
DEV_DESC = "device_description"
cols = [DEV_ID, DEV_MAN, DEV_DESC]


def align_manufacturer(new_tab: pd.DataFrame) -> pd.DataFrame:
    # AVOID DUPLICATES OF DEVICE_ID BECOUSE OF DIFFERENT MANUFACTURERS
    # group by device_id
    # report all cases when are more manufacturers for the same device_id
    # - merge to one with longer description
    # - keep new manufacturer
    dat = getDF(tab="DEVICE")
    if dat.empty:
        return new_tab
    # 1. split new_tab into three parts:
    # 1.1. new_tab with device_id not in dat = mod_tab
    mod_tab = new_tab[~new_tab[DEV_ID].isin(dat[DEV_ID])]
    # 1.2. new_tab with device_id and device_manufacturer in dat + mod_tab = mod_tab
    # take longer description
    mod_tab = pd.concat(
        [
            mod_tab, 
            long_description(new_tab, dat, by=[DEV_ID, DEV_MAN])]
    )
    # 1.3. what left:  new_tab - mod_tab = check_tab
    check_tab = new_tab[~new_tab[DEV_ID].isin(mod_tab[DEV_ID])]
    
    # show duplicates of device_id
    # device_id | device_manufacturer1 | device_description1
    # device_id | device_manufacturer2 | device_description2
    # etc
    print(
        pd.concat(
            [
                check_tab,
                dat[dat[DEV_ID].isin(check_tab[DEV_ID])]
            ]).
        sort_values(by=[DEV_ID]).
        set_index(DEV_ID, append=True)
        [cols[1:]]
    )

    # 4. ask user for decision: merge, keep, iterate
    print("[m]erge will leave only lines with longer description")
    print("[k]eep will add new device_manufacturer")
    i = input("What to do? (m)erge, (k)eep: ")

    if i == "m":
        # merging
        # group check_tab by device_id,
        check_tab = long_description(check_tab, dat, by=[DEV_ID])
        return pd.concat([mod_tab, check_tab], ignore_index=True)
    if i == "k":
        # keep
        # we keep new manufacturer for the same device_id
        ...


def long_description(
    tab1st: pd.DataFrame, tab2nd: pd.DataFrame, by=list[str]
) -> pd.DataFrame:
    # take the longest description for each group by device_id
    # return first row from group
    # from tab2nd take only rows with device_id and device_manufacturer in tab1st
    tab2nd = tab2nd.merge(tab1st,on=by,how='inner')
    tab = pd.concat([tab1st, tab2nd], ignore_index=True)
    tab = (tab.
           groupby(DEV_ID).
           filter(lambda x: len(x) > 1)
           )
    tab = tab.groupby(DEV_ID)

    for name, group in tab:
        # take the longest description
        row = group.loc[
                    group[DEV_DESC].
                    apply(lambda x: len(x) if pd.notnull(x) else 0).
                    idxmax()
                    ]
        group[DEV_DESC] = row[DEV_DESC]
        group[DEV_MAN] = row[DEV_MAN]
    return tab.nth(1)
