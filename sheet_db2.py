# %%
import gspread
import pandas as pd
from peewee import (
    SqliteDatabase, IntegrityError, fn
    )
from google.oauth2.service_account import Credentials
from db import Company, CoalCompanyPerformance, MiningSite, CompanyOwnership
import re
from tabulate import tabulate

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
c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')

c_data = c_sheet.get('A1:R250')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])
c_df['company_name_cleaned'] = c_df['name'].apply(clean_company_name)

# %%
deleted_companies = []

for company in Company.select():
    company_name_cleaned = clean_company_name(company.name)
    if company_name_cleaned not in c_df['company_name_cleaned'].values:
        deleted_companies.append((company.id, company.name))

print(f"Deleted company list: \n{deleted_companies}")

for d_c_id, d_c_name in deleted_companies:
    for table in (CoalCompanyPerformance, MiningSite):
        qs = table.select().where(table.company == d_c_id)
        if qs:
            print(f"Found record found in {table} table")
            for row in qs:
                print('\t', table, row)
        else:
            print(f"\nNo record found in {table} table")

if deleted_companies and \
    input("Sure you want to delete rows on deleted companies list? [Y/N]") == 'Y':
    
    with db.atomic():
        for d_c_id, d_c_name in deleted_companies:
            try:
                row = Company.get_by_id(int(d_c_id))
                row.delete_instance()
                print(f"Deleted company: {d_c_name} (ID: {d_c_id})")
            except Company.DoesNotExist:
                print(f"Company with ID {d_c_id} does not exist. Skipping.")

# %%
new_c_df = c_df[~c_df['company_name_cleaned'].isin(existing_companies)]

ignore_companies = [
    # 'Delta Dunia Makmur Tbk'
]

new_c_df = new_c_df[~new_c_df['name'].isin(ignore_companies)]
null_ko = new_c_df[new_c_df['key_operation'].isna()]

print(f"Companies with NULL key operation: \n{null_ko} \n\n")

new_c_df = new_c_df[~new_c_df['key_operation'].isna()]

print(f"Companies to be added: \n{new_c_df}")

# %%

def safe_value(val):
    return None if (pd.isna(val) or val == "") else val

# %%
if len(new_c_df) and \
    input("Adding new companies, please confirm [Y/N]") == "Y":

    latest_row = None

    try:
        with db.atomic():
            for _, row in new_c_df.iterrows():
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

# %%a
existing_c_df = c_df[c_df['company_name_cleaned'].isin(existing_companies)]

fields_to_compare = [
    'name',
    'idx_ticker',
    'operation_province',
    'operation_kabkot',
    'representative_address',
    'company_type',
    'key_operation',
    'activities',
    'website',
    'phone_number',
    'email'
]

def checkCompanyEdits(execute=False):
    edit_exist = False

    for row_id, row in existing_c_df.iterrows():
        company = Company.get_or_none(
            fn.LOWER(fn.TRIM(fn.REPLACE(fn.REPLACE(Company.name, 'PT', ''), 'Tbk', ''))) == row['company_name_cleaned']
            )

        if company:
            differences = []
            for field in fields_to_compare:
                model_value = getattr(company, field)
                df_value = safe_value(row[field])

                if (model_value != df_value) and (df_value is not None):
                    differences.append([field, model_value, df_value])
                    if execute:
                        setattr(company, field, df_value) 
                        print(f"{field} for {company.name} has been updated to {df_value}")

            if differences:
                edit_exist = True

                if execute:
                    company.save()
                else:
                    print(f"\nDifferences for company '{company.name}':")
                    print(tabulate(differences, headers=["Field", "DB Value", "CSV Value"], tablefmt="grid"))   

    return edit_exist

if checkCompanyEdits() and \
    (input("Do you want to apply these updates to the DB? [Y/N]") == "Y"):

    checkCompanyEdits(execute=True)

# %%

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance')

ccp_data = ccp_sheet.get('A1:Y135')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])
ccp_df = ccp_df.rename(columns={"total_reserve": "reserve", "total_resource":"resource"})

# %%

def syncCompanyNameAndID(df, sheet, company_id_col='company_id', company_name_col='*company_name', execute=False):
    unsync_exist = False
    for row_id, row in df.iterrows():

        if (row[company_id_col] is None) or (row[company_id_col] == ''):
            c_q = Company.get_or_none(Company.name == row[company_name_col])
            
            if c_q:
                if c_q.id != row[company_id_col]:
                    unsync_exist = True

                    if execute:
                        c_id_col_idx = df.columns.get_loc(company_id_col)
                        sheet.update_cell(2 + row_id, c_id_col_idx + 1, c_q.id)

                        print(f"Company id has been filled for {row[company_name_col]} with ID: {c_q.id}")

                    else:
                        print(f"Company id to be added on Sheet for {row[company_name_col]}, ID: {c_q.id}")

        else:
            c_q = Company.get_or_none(Company.id == row[company_id_col])

            if c_q:
                if c_q.name != row[company_name_col]:
                    unsync_exist = True
                    
                    if execute:
                        c_name_col_idx = df.columns.get_loc(company_name_col)
                        sheet.update_cell(2 + row_id, c_name_col_idx + 1, c_q.name)

                        print(f"coal_company_performance sheet on row, col: {2 + row_id}, {c_name_col_idx + 1} has been updated to {c_q.name}")

                    else:
                        diff = []
                        diff.append((c_q.name, row[company_name_col]))

                        print(f"Different naming detected on row {row_id}:\n", tabulate(diff, headers=["DB Value", "Sheet Value"], tablefmt="grid"))   
            else:
                print("Unlisted company:", row[company_name_col])
    
    return unsync_exist

# %%

if syncCompanyNameAndID(ccp_df, ccp_sheet) and \
    (input("Sycn company names? [Y/N]") == "Y"):
    syncCompanyNameAndID(ccp_df, ccp_sheet, execute=True)

# %%

def checkCPPEditAndInsert(execute=False):

    change_exist = False

    fields_to_compare = [
        ('mineral_type', str),
        ('calorific_value', str),
        ('mining_operation_status', str),
        ('mining_permit', str),
        ('area', int),
        ('production_volume', float),
        ('sales_volume', float),
        ('overburden_removal_volume', float),
        ('strip_ratio', float),
        ('reserve', float),
        ('resource', float),
        ('mineral_sub_type', str),
        ('area_geometry', str)
    ]

    def safe_value(val, tp):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    for row_id, row in ccp_df.iterrows():
        ccp_q = CoalCompanyPerformance.get_or_none(
            (CoalCompanyPerformance.company == row['company_id']) & 
            (CoalCompanyPerformance.year == row['year'])
        )

        if ccp_q:
            differences = []
            for field, field_type in fields_to_compare:
                model_value = getattr(ccp_q, field)
                df_value = safe_value(row[field], field_type)

                if (model_value != df_value):
                    differences.append([field, model_value, df_value])

                    if execute:
                        setattr(ccp_q, field, df_value)

            if differences:

                change_exist = True

                if execute:
                    for diff in differences:
                        print(f"{diff[0]} for {ccp_q.company.name} {ccp_q.year} has been updated to {diff}")
                    ccp_q.save()

                else:
                    print(f"\nDifferences for company, year '{ccp_q.company.name} {ccp_q.year}':")
                    print(tabulate(differences, headers=["Field", "DB Value", "CSV Value"], tablefmt="grid"))
        else:

            print(row['company_id'])

            if execute == False:
                print("New Data:", row['*company_name'])
            
            change_exist = True

            if execute:
                try:
                    with db.atomic():

                        q_c = Company.get_or_none(Company.name == row['*company_name'])

                        if q_c:
                            q_c_id = q_c.id

                            ccp = CoalCompanyPerformance(
                                company=q_c_id,
                                year=safe_value(row['year'], int),
                                mineral_type=safe_value(row['mineral_type'], str),
                                calorific_value=safe_value(row['calorific_value'], str),
                                mining_operation_status=safe_value(row['mining_operation_status'], str),
                                mining_permit=safe_value(row['mining_permit'], str),
                                area=safe_value(row['area'], int),
                                production_volume=safe_value(row['production_volume'], float),
                                sales_volume=safe_value(row['sales_volume'], float),
                                overburden_removal_volume=safe_value(row['overburden_removal_volume'], float),
                                strip_ratio=safe_value(row['strip_ratio'], float),
                                reserve=safe_value(row['reserve'], float),
                                resource=safe_value(row['resource'], float),
                                mineral_sub_type=safe_value(row['mineral_sub_type'], str),
                                area_geometry=safe_value(row['area_geometry'], str),
                            )
                            ccp.save()

                            print(f"Inserted at ID: {ccp.id}")

                        else:
                            print(f"{row['*company_name']} is unlisted")

                except Exception as e:
                    print(f"Transaction failed: {e}")

                else:
                    print(f"{row['*company_name']} added successfully!")

    return change_exist

if checkCPPEditAndInsert() and \
    (input("Apply changes? [Y/N]") == "Y"):
    checkCPPEditAndInsert(execute=True)
 # %%

def syncCCPID(execute=False):
    unsync_exist = False

    def safeCast(val, tp):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    for row_id, row in ccp_df.iterrows():
        ccp_q = CoalCompanyPerformance.get_or_none(
            (CoalCompanyPerformance.company == row['company_id']) & 
            (CoalCompanyPerformance.year == row['year'])
        )

        if ccp_q.id != safeCast(row['id'], int):

            unsync_exist = True

            if (row['id'] is None) or (row['id'] == ''):
                if execute:
                    id_col_idx = ccp_df.columns.get_loc('id')
                    ccp_sheet.update_cell(2 + row_id, id_col_idx + 1, ccp_q.id)
                    print(f"Synced Coal Company Performance ID: {ccp_q.id} for company name, year: {row['*company_name']}, {row['year']}")
                else:
                    print(f"Need to fill in row ID for company name, year: {row['*company_name']}, {row['year']}")
            else:
                print("Unsync row at", row_id + 2, ccp_q.id, row['id'])

    return unsync_exist

if syncCCPID() and \
    (input("Sync coal_company_performance sheet id with DB? [Y/N]") == "Y"):
    syncCCPID(execute=True)
# %%

if syncCompanyNameAndID(c_df, c_sheet, company_id_col='*parent_company_id', company_name_col='*parent_company_name') and \
    (input("Sycn company names? [Y/N]") == "Y"):
    syncCompanyNameAndID(c_df, c_sheet, company_id_col='*parent_company_id', company_name_col='*parent_company_name', execute=True)

# %%

def replaceCO(df):

    def safe_value(val, tp=int):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    CompanyOwnership.delete().execute()

    print("All Company Ownership records deleted")

    for _, row in df.iterrows():

        print(row['id'])

        try:
            with db.atomic():

                parent = Company.get_or_none(Company.name == row['*parent_company_name'])
                company = Company.get_or_none(Company.name == row['name'])

                if parent and company:

                    CompanyOwnership.insert(
                        parent=parent.id,
                        company=company.id,
                        percentage_ownership=safe_value(row['percentage_ownership'], float)
                    ).execute()

                    print(f"Inserted for: {parent.id} and {company.id}")

                else:
                    print(f"{row['*parent_company_name']} and / or {row['name']} is unlisted")

        except Exception as e:
            print(f"Transaction failed: {e}")

        else:
            print(f"{row['*parent_company_name']} and {row['name']} added succesfully!")

if (input("Replace company ownerhip according to the sheet?") == "Y"):
    replaceCO(c_df)
# %%# %%

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')

ms_data = ms_sheet.get('A1:W43')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])
ms_df = ms_df.rename(columns={"total_reserve": "reserve", "total_resource":"resource"})

if syncCompanyNameAndID(ms_df, ms_sheet) and \
    (input("Sycn company names? [Y/N]") == "Y"):
    syncCompanyNameAndID(ms_df, ms_sheet, execute=True)

# %%

ms_field_type = [
        ('name', str),
        ('year', int),
        ('calorific_value', str),
        ('production_volume', float),
        ('overburden_removal_volume', float),
        ('strip_ratio', float),
        ('reserve', float),
        ('resource', float),
        ('province', str),
        ('city', str),
        ('mineral_type', str)
    ]

def checkMSEditAndInsert(df, fields_to_compare, execute=False):

    change_exist = False

    def safe_value(val, tp):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    for row_id, row in df.iterrows():
        q = MiningSite.get_or_none(
            (MiningSite.name == row['name']) &
            (MiningSite.company == row['company_id']) & 
            (MiningSite.year == row['year'])
        )

        if q:
            differences = []
            for field, field_type in fields_to_compare:
                model_value = getattr(q, field)
                df_value = safe_value(row[field], field_type)

                if (model_value != df_value):
                    differences.append([field, model_value, df_value])

                    if execute:
                        setattr(q, field, df_value)

            if differences:

                change_exist = True

                if execute:
                    for diff in differences:
                        print(f"{diff[0]} for {q.name} {q.company.name} {q.year} has been updated to {diff[2]}")
                    q.save()

                else:
                    print(f"\nDifferences for company, year '{q.name} {q.company.name} {q.year}':")
                    print(tabulate(differences, headers=["Field", "DB Value", "CSV Value"], tablefmt="grid"))
        else:

            if execute == False:
                print("New Data:", row['id'], row['name'], row['company_id'], row['*company_name'], row['year'])
            
            change_exist = True

            if execute:
                try:
                    with db.atomic():

                        q_c = Company.get_or_none(Company.name == row['*company_name'])

                        if q_c:
                            ms = MiningSite(
                                name=safe_value(row['name'], str),
                                year=safe_value(row['year'], int),
                                company=q_c.id,
                                calorific_value=safe_value(row['calorific_value'], str),
                                production_volume=safe_value(row['production_volume'], float),
                                overburden_removal_volume=safe_value(row['overburden_removal_volume'], float),
                                strip_ratio=safe_value(row['strip_ratio'], float),
                                reserve=safe_value(row['reserve'], float),
                                resource=safe_value(row['resource'], float),
                                province=safe_value(row['province'], str),
                                city=safe_value(row['city'], str),
                                mineral_type=safe_value(row['mineral_type'], str),
                            )
                            ms.save()

                            print(f"Inserted at ID: {ms.id}")

                        else:
                            print(f"{row['*company_name']} is unlisted")

                except Exception as e:
                    print(f"Transaction failed: {e}")

                else:
                    print(f"{row['*company_name']} added successfully!")

    return change_exist


if checkMSEditAndInsert(ms_df, ms_field_type) and \
    (input("Apply changes? [Y/N]") == "Y"):
    checkMSEditAndInsert(ms_df, ms_field_type, execute=True)
# %%

def syncID(df, sheet, execute=False):
    unsync_exist = False

    def safeCast(val, tp):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    for row_id, row in df.iterrows():
        q = MiningSite.get_or_none(
            (MiningSite.name == row['name']) &
            (MiningSite.company == row['company_id']) & 
            (MiningSite.year == row['year'])
        )

        if q.id != safeCast(row['id'], int):

            unsync_exist = True

            if (row['id'] is None) or (row['id'] == ''):
                if execute:
                    id_col_idx = df.columns.get_loc('id')
                    sheet.update_cell(2 + row_id, id_col_idx + 1, q.id)
                    print(f"Synced Mining Site ID: {q.id} for site name, company name, year: {row['name']} {row['*company_name']}, {row['year']}")
                else:
                    print(f"Need to fill in row ID for site name, company name, year: {row['name']}, {row['*company_name']}, {row['year']}")
            else:
                print("Unsync row at", row_id + 2, q.id, row['id'])

    return unsync_exist

if syncID(ms_df, ms_sheet) and \
    (input("Sync mining_site sheet id with DB? [Y/N]") == "Y"):
    syncID(ms_df, ms_sheet, execute=True)
# %%