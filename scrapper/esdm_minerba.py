# This is a script to scrape data from https://geoportal.esdm.go.id/minerba/#

import requests
import pandas as pd

url = "https://geoportal.esdm.go.id/monaresia/sharing/servers/d53b4575aa1b40fbbacdd244caeee468/rest/services/Pusat/WIUP_Publish/MapServer/0/query"

def constructUrlAndParams(query_filter):
    params = {
            "f": "json",
            "returnGeometry": "true",
            "spatialRel": "esriSpatialRelIntersects",
            "geometry": '{"xmin":9904297.370044464,"ymin":-2433296.715522152,"xmax":16224722.364887437,"ymax":2302130.0607998283,"spatialReference":{"wkid":102100}}',
            "geometryType": "esriGeometryEnvelope",
            "inSR": "102100",
            "outFields": "*",
            "orderByFields": "objectid ASC",
            "outSR": "102100",
            "resultRecordCount": "90"
        }
    
    for key, value in query_filter.items():
        params[key] = value
    
    return url, params

def toPandasDf(data):
    df = pd.DataFrame(data)
    return df

def scrape(data_count:int, query_filter:dict):

    page_df_list = []

    for i in range(0, data_count, 90):
        
        query_filter['resultOffset'] = f"{i}"

        url, params = constructUrlAndParams(query_filter)

        response = requests.get(url, params=params)
        
        if response.ok:
            data = response.json()

            single_page_data_point_list = []
            
            for d in data['features']:
                data_point = d['attributes']
                data_point['geometry'] = d['geometry']['rings']
            
                single_page_data_point_list.append(data_point)
            
            single_page_df = pd.DataFrame(single_page_data_point_list)
            page_df_list.append(single_page_df)

        else:
            print("Request failed:", response.status_code)

    minerba_df = pd.concat(page_df_list)
    minerba_df.reset_index(drop=True, inplace=True)

    return minerba_df

nickel_filter = {
    "data_count": 340, 
    "query_filter":
        {
            "where": "(LOWER(komoditas) LIKE '%nikel%')"
        }
    }

batubara_filter = {
    "data_count": 954, 
    "query_filter":
        {
            "where": "(LOWER(komoditas) LIKE '%batubara%')"
        }
    }

all_filter = {
    "data_count": 8300, 
    "query_filter":{}
    }

nickel_df = scrape(**nickel_filter)

nickel_df.to_csv(f"scrapper/esdm_minerba-nickel.csv")