# %%
from sheet_api.utils.dataframe_utils import safeCast
from sheet_api.google_sheets.auth import createClient, createService
from minerba_merge import prepareMinerbaDf

import json
import re

_, spreadsheet_id = createClient()
service = createService()


# %%
def compileToJsonBatch(df, included_columns, target_col, sheet_id, starts_from=203):

    col_id = df.columns.get_loc(target_col)

    rows = []

    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        data_dict = {}

        for in_col, type in included_columns:
            val = safeCast(row[in_col], type)

            in_col_cleaned = in_col.lstrip("*")

            data_dict[in_col_cleaned] = val

        rr_cols_json = json.dumps(data_dict)
        to_use_value = {"stringValue": f"{rr_cols_json}"}

        rows.append({"values": [{"userEnteredValue": to_use_value}]})

    # requests = [
    #     {
    #         'updateCells': {
    #             'range': {
    #                 'sheetId': 147673991,
    #                 'startRowIndex': starts_from + 1,
    #                 'endRowIndex': 240 + 1,
    #                 'startColumnIndex': col_id,
    #                 'endColumnIndex': col_id + 1
    #             },
    #             'rows': rows,
    #             'fields': 'userEnteredValue'
    #         }
    #     }
    # ]

    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"rowCount": 300, "columnCount": 43},
                },
                "fields": "gridProperties(rowCount,columnCount)",
            }
        },
        {
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": starts_from + 3,
                    "endRowIndex": 241 + 3,
                    "startColumnIndex": col_id,
                    "endColumnIndex": col_id + 1,
                },
                "rows": rows,
                "fields": "userEnteredValue",
            }
        },
    ]

    response = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute()
    )

    print(f"Batch update response: {response}")


def clean_company_name(name):
    return re.sub(r"\b(PT|Tbk)\b", "", name).lower().strip()


def fillMiningLicense(df, sheet_id, starts_from=0):

    minerba_df, included_columns = prepareMinerbaDf()

    col_id = df.columns.get_loc("mining_license")

    rows = []

    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = minerba_df[
            minerba_df["company_name"].str.lower() == clean_company_name(row["name"])
        ]

        if not company_q.empty:
            records = company_q[included_columns].to_dict(orient="records")
        else:
            records = []  # empty list when no matches

        # ### CHANGED: dump the list (even if empty) as your JSON array
        license_json = json.dumps(records, ensure_ascii=False)
        to_use_value = {"stringValue": license_json}

        # if not company_q.empty:
        #     records = company_q[included_columns].to_dict(orient="records")
        #     license_json = json.dumps(records, ensure_ascii=False)  # array of objects
        #     to_use_value = {"stringValue": license_json}
        #     # license_json = company_q[included_columns].iloc[0].to_json()
        #     # to_use_value = {'stringValue':f'{license_json}'}

        # else:
        #     to_use_value = {}

        rows.append({"values": [{"userEnteredValue": to_use_value}]})

        # sheet.update_cell(2 + row_id, col_id + 1, license_json)
        # print(f"Updated row {row_id + 2}: {license_json}")

    requests = [
        {
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": starts_from + 1,
                    "endRowIndex": len(df) + 1,
                    "startColumnIndex": col_id,
                    "endColumnIndex": col_id + 1,
                },
                "rows": rows,
                "fields": "userEnteredValue",
            }
        }
    ]

    response = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute()
    )

    print(f"Batch update response: {response}")


# %%

mining_site_resources_reserves_cols_type = [
    ("*year_measured", int),
    ("*calorific_value", str),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float),
]

mining_site_location_cols_type = [
    ("*province", str),
    ("*city", str),
    ("*latitude", float),
    ("*longitude", float),
]

company_performance_resources_reserves_cols_type = [
    ("*year_measured", int),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float),
]

company_performance_coal_stats_type = [
    ("mining_operation_status", str),
    ("*production_volume", float),
    ("*sales_volume", float),
    ("*overburden_removal_volume", float),
    ("*strip_ratio", float),
    ("*resources_reserves", dict),
    ("*product", dict),
]

mineral_reserves_cols_type = [
    ("ore_reserves material (mt)", float),
    ("ore_reserves g/ton Au (koz)", float),
    ("ore_reserves Au (koz)", float),
    ("ore_reserves g/ton Ag (koz)", float),
    ("ore_reserves Ag (koz)", float),
    ("ore_reserves % Cu", float),
    ("ore_reserves Cu (mt)", float),
]

mineral_resources_cols_type = [
    ("resources material (mt)", float),
    ("resources g/ton Au (koz)", float),
    ("resources Au (koz)", float),
    ("resources g/ton Ag (koz)", float),
    ("resources Ag (koz)", float),
    ("resources % Cu", float),
    ("resources Cu (mt)", float),
]

mineral_reserves_resources_cols_type = [
    ("*year_measured", int),
    ("ore_reserves", dict),
    ("resources", dict),
]

mineral_commodity_stats_type = [
    ("*unit", str),
    ("mining_operation_status", str),
    ("*production_volume", float),
    ("*sales_volume", float),
    ("*resources_reserves", dict),
    ("*product", dict),
]
