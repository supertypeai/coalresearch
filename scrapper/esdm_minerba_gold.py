import requests
import pandas as pd
import json  # Make sure to keep this import if you want the debug print for JSON head

# --- REVISED URL HERE ---
url = "https://geoportal.esdm.go.id/monaresia/sharing/servers/48852a855c014a63acfd78bf06c2689a/rest/services/Pusat/WIUP_Publish/MapServer/0/query"
# --- END REVISED URL ---


def constructUrlAndParams(query_filter):
    print("\n--- Entering constructUrlAndParams function ---")
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
        "resultRecordCount": "90",
    }

    for key, value in query_filter.items():
        params[key] = value

    print(f"Constructed URL: {url}")
    print(f"Constructed Parameters: {params}")
    print("--- Exiting constructUrlAndParams function ---")
    return url, params


def toPandasDf(data):
    print("\n--- Entering toPandasDf function ---")
    df = pd.DataFrame(data)
    print("--- Exiting toPandasDf function ---")
    return df


def scrape(data_count: int, query_filter: dict):
    print(f"\n--- Starting scrape function for data_count: {data_count} ---")
    print(f"Initial query filter: {query_filter}")

    page_df_list = []

    for i in range(0, data_count, 90):
        print(f"\n--- Scraping offset: {i} ---")

        query_filter["resultOffset"] = f"{i}"

        url, params = constructUrlAndParams(query_filter)

        print(f"Making request to: {url} with params: {params}")
        response = requests.get(url, params=params)

        if response.ok:
            data = response.json()
            print(f"Request successful! Status code: {response.status_code}")

            # --- DEBUG PRINT: Head of the JSON response data ---
            print("\n--- Head of JSON response data: ---")
            if (
                isinstance(data, dict)
                and "features" in data
                and isinstance(data["features"], list)
            ):
                # Print the first 2 features if available
                print(json.dumps(data["features"][:2], indent=2))
                if len(data["features"]) > 2:
                    print("... (more features available)")
            else:
                # Print the entire dictionary if 'features' key is not found or not a list
                # or just a snippet if it's too large
                print(
                    json.dumps(data, indent=2)[:500] + "..."
                    if len(json.dumps(data, indent=2)) > 500
                    else json.dumps(data, indent=2)
                )
            print("------------------------------------\n")
            # --- END DEBUG PRINT ---

            single_page_data_point_list = []

            if "features" in data:
                for d in data["features"]:
                    data_point = d["attributes"]
                    data_point["geometry"] = d["geometry"]["rings"]

                    single_page_data_point_list.append(data_point)
                print(
                    f"Fetched {len(single_page_data_point_list)} records for this page."
                )
            else:
                print("No 'features' found in the response data.")

            single_page_df = pd.DataFrame(single_page_data_point_list)
            page_df_list.append(single_page_df)

        else:
            print("Request failed:", response.status_code)
            print("Response text:", response.text)

    minerba_df = pd.concat(page_df_list)
    minerba_df.reset_index(drop=True, inplace=True)
    print(f"\n--- Scraping finished. Total records scraped: {len(minerba_df)} ---")
    print("--- Exiting scrape function ---")
    return minerba_df


# Note: The 'emas' filter will now correctly use the new URL.
emas_filter = {
    "data_count": 950,
    "query_filter": {"where": "(LOWER(komoditas) LIKE '%emas%')"},
}

nickel_filter = {
    "data_count": 340,
    "query_filter": {"where": "(LOWER(komoditas) LIKE '%nikel%')"},
}

batubara_filter = {
    "data_count": 954,
    "query_filter": {"where": "(LOWER(komoditas) LIKE '%batubara%')"},
}

all_filter = {"data_count": 8300, "query_filter": {}}

print("\n--- Calling scrape for emas data ---")
emas_df = scrape(**emas_filter)  # Changed to scrape 'emas' data

print("\n--- Saving emas data to CSV ---")
# Changed filename to reflect 'emas' data
emas_df.to_csv(f"scrapper/esdm_minerba-emas2.csv", index=False)
print("--- Emas data saved successfully! ---")

# You can uncomment and run other filters if needed
# print("\n--- Calling scrape for nickel data ---")
# nickel_df = scrape(**nickel_filter)
# nickel_df.to_csv(f"scrapper/esdm_minerba-nickel.csv", index=False)
# print("--- Nickel data saved successfully! ---")
