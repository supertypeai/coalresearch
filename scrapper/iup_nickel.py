# This is a script to scrape data from https://www.apni.or.id/en/iupnikel

import pandas as pd
import requests

url = "https://apiform88.herokuapp.com/dataIUP/dataMinerals"

response = requests.get(url)

def toPandasDf(data):
    df = pd.DataFrame(data)
    return df

def scrape():

    df = pd.DataFrame([])

    if response.ok:
        data = response.json()
        data = data['data'] 

        df = toPandasDf(data)

    else:
        print("Request failed:", response.status_code)

    return df


iup_nickel_df = scrape()

iup_nickel_df.to_csv(f"scrapper/iup_nickel.csv")