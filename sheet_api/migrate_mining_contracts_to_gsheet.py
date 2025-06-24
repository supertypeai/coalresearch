import pandas as pd
import json
from google_sheets.auth import createClient

# Authenticate and get clients
client, spreadsheet_id = createClient()


# Helper to deduplicate headers
def deduplicate_headers(headers):
    seen = {}
    unique_headers = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)
    return unique_headers


# Read mining_contract sheet
mining_contract_sheet = client.open_by_key(spreadsheet_id).worksheet("mining_contract")
mc_raw_values = mining_contract_sheet.get_all_values()
mc_headers = deduplicate_headers(mc_raw_values[0])
mining_contract_df = pd.DataFrame(mc_raw_values[1:], columns=mc_headers)

# Read company sheet
company_sheet = client.open_by_key(spreadsheet_id).worksheet("company")
c_raw_values = company_sheet.get_all_values()
c_headers = deduplicate_headers(c_raw_values[0])
company_df = pd.DataFrame(c_raw_values[1:], columns=c_headers)

# Ensure 'mining_contract' column exists in company_df
if "mining_contract" not in company_df.columns:
    company_df["mining_contract"] = ""

# Clean and normalize IDs for reliable matching
mining_contract_df["contractor_id"] = pd.to_numeric(
    mining_contract_df["contractor_id"], errors="coerce"
)
mining_contract_df.dropna(subset=["contractor_id"], inplace=True)
mining_contract_df["contractor_id"] = (
    mining_contract_df["contractor_id"].astype(int).astype(str)
)
company_df["id"] = company_df["id"].astype(str)

# Group contracts by contractor_id
grouped = mining_contract_df.groupby("contractor_id")

# Create a mapping from company id to its index for efficient updates
id_to_index_map = {id_val: index for index, id_val in company_df["id"].items()}

# For each contractor, aggregate their contracts into a JSON array with the new structure
for contractor_id, group in grouped:
    if contractor_id in id_to_index_map:
        idx = id_to_index_map[contractor_id]

        contract_list = []
        for _, row in group.iterrows():
            agreement_type_str = row.get("Agreement type", "")
            agreement_types = (
                [item.strip() for item in agreement_type_str.split(",")]
                if agreement_type_str
                else []
            )

            new_contract = {
                "company_name": row.get("*mine_owner_name"),
                "company_id": row.get("mine_owner_id"),
                "contract_period_end": row.get("contract_period_end"),
                "agreement_type": agreement_types,
            }
            contract_list.append(new_contract)

        contracts_json = json.dumps(contract_list)
        company_df.at[idx, "mining_contract"] = contracts_json

# Fill empty mining_contract cells with '[]'
company_df["mining_contract"] = company_df["mining_contract"].fillna("[]")
company_df.loc[company_df["mining_contract"] == "", "mining_contract"] = "[]"

# Convert dataframe to list of lists to update the sheet
updated_values = [company_df.columns.values.tolist()] + company_df.fillna(
    ""
).values.tolist()

# Update the entire 'company' sheet
company_sheet.clear()
company_sheet.update(
    range_name="A1", values=updated_values, value_input_option="USER_ENTERED"
)

print("Updated 'company' sheet with restructured mining contract JSON data.")
