# %%
import gspread
import pandas as pd
from peewee import (
    SqliteDatabase, IntegrityError, fn
    )
from google.oauth2.service_account import Credentials
from db import Company, CoalCompanyPerformance
import re

# %%
# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = 'keys/supertype-insider-d4710ac3561a.json'

# Define scope
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
spreadsheet_id = "19wfJ2fc9qKeR22dMIO2rEQLkit8E4bGsHA1u0USqTQk"

def clean_company_name(name):
    return re.sub(r'\b(PT|Tbk)\b', '', name).lower().strip()

db = SqliteDatabase('coal_db.sqlite')
existing_companies = [clean_company_name(company.name) for company in Company.select()]

# %%
c_sheet = client.open_by_key(spreadsheet_id).worksheet('company_temp')

c_data = c_sheet.get('A1:M143')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

c_df['company_name_cleaned'] = c_df['name'].apply(clean_company_name)
new_ccp_df = c_df[~c_df['company_name_cleaned'].isin(existing_companies)]

ignore_companies = [
    'Delta Dunia Makmur Tbk'
]

new_ccp_df = new_ccp_df[~new_ccp_df['name'].isin(ignore_companies)]
null_ko = new_ccp_df[new_ccp_df['key_operation'].isna()]

print(f"Companies with NULL key operation: \n{null_ko} \n\n")

new_ccp_df = new_ccp_df[~new_ccp_df['key_operation'].isna()]

print(f"Companies to be added: \n{new_ccp_df}")

# %%

def safe_value(val):
    return None if (pd.isna(val) or val == "") else val

latest_row = None

try:
    with db.atomic():
        for _, row in new_ccp_df.iterrows():
            latest_row = row

            company = Company(
                name=safe_value(row['name']),
                idx_ticker=safe_value(row['idx_ticker']),
                operation_province=safe_value(row['operation_province']),
                operation_kabkot=safe_value(row['operation_kabkot']),
                representative_address=safe_value(row['representative_address']),
                company_type=safe_value(row['company_type']),
                key_operation=safe_value(row['key_operation']),
                activities=safe_value(row['activities']),
                website=safe_value(row['website']),
                phone_number=safe_value(row['phone_number']),
                email=safe_value(row['email']),
            )
            company.save()

except IntegrityError as e:
    print(f"Transaction failed: {e}")
    print(f"Latest row processed: {latest_row}")

else:
    print("All companies added successfully!")
# %%
ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance')

ccp_data = ccp_sheet.get('A1:V79')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])

# %%
c_ccp_q = (
    CoalCompanyPerformance
    .select(
        fn.CONCAT(Company.name, CoalCompanyPerformance.year).alias('companyyear'),
    )
    .join(Company, on=(CoalCompanyPerformance.company == Company.id))
)

existing_companyyear = [row.companyyear for row in c_ccp_q]

# %%

c_ccp_df = pd.merge(c_df[['id-new2', 'name']], ccp_df, left_on='id-new2', right_on='company_id', how='inner')
c_ccp_df = c_ccp_df[(c_ccp_df['production_year'].notna()) & (c_ccp_df['production_year'] != '')]
c_ccp_df['companyyear'] = (
    c_ccp_df['name'].astype(str) +
    c_ccp_df['production_year'].astype(str)
)

# %%
new_ccp_df = c_ccp_df[~c_ccp_df['companyyear'].isin(existing_companyyear)]

ignore_companyyear = [
    'Delta Dunia Makmur Tbk2023'
]

new_ccp_df = new_ccp_df[~new_ccp_df['companyyear'].isin(ignore_companyyear)]
print(f"Companies to be added: \n{new_ccp_df}")

# %%
