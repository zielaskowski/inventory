import pandas as pd

from app.sql import getDF


def align_manufacturer(new_tab: pd.DataFrame) -> pd.DataFrame:
    # group by device_id
    # report all cases when are more manufacturers for the same device_id
    # - just inform
    # - merge to one with longer description
    # - ask user for decision
    dat = getDF(tab="DEVICE")
    # merge dat and new_tab so to leave only common device_id rows
    dat_m = pd.merge(dat, new_tab, on="device_id", how="inner")
    # leave only columns with device_id and manufacturer
    print(dat[["device_id", "manufacturer"]])
    i = input("What to do? [m]erge, [n]othing, [i]interate")
    if i == "m": # merge
        # add new_tab to dat
        dat = pd.concat([dat, new_tab])
        # group by device and remove single device_id rows
        dat = dat.groupby("device_id").apply(lambda x: len(x)>1)
        # group by device_id and take only one row with longest description
        dat = dat.groupby("device_id").apply(lambda x: x.loc[x["device_description"].idxmax()])
        # remove from new_tab all rows with device_id in dat_m
        new_tab = new_tab[~new_tab["device_id"].isin(dat_m["device_id"])]
        new_tab = pd.concat([dat, new_tab])
        return new_tab
    elif i == "n":
        return new_tab
    elif i == "i":
        # iterate over dat_m
        for index, row in dat_m.iterrows():
            print(row)
            i = input("What to do? [m]erge, [n]othing, [i]next")
            if i == "m":
                # merge
                # remove from new_tab all rows with device_id in dat_m
                new_tab = new_tab[~new_tab["device_id"].isin(dat_m["device_id"])]
                # add new_tab to dat
                dat = pd.concat([dat, new_tab])

    
