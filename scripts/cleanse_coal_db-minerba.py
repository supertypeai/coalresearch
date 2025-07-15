import pandas as pd
import os
import sys

sys.path.append(os.getcwd())

from scrapper.esdm_minerba import cleanse_df

df = pd.read_csv("coal_db - minerba.csv")
df.drop(df.columns[0], axis=1, inplace=True)
df.insert(0, '', range(len(df)))
df = cleanse_df(df)

nan_cols = ["cnc", "generasi", "kode_wil", "lokasi"]

for col in nan_cols:
    if not df[df[col] == 'nan'].empty:
        df.loc[df[col] == 'nan', col] = ""

df.to_csv("coal_db - minerba (cleansed).csv", index=False)