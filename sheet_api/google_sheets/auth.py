import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SERVICE_ACCOUNT_FILE = 'keys/supertype-insider-d4710ac3561a.json'

# Define scope
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

def createClient(): 
    client = gspread.authorize(creds)
    # spreadsheet_id = "19wfJ2fc9qKeR22dMIO2rEQLkit8E4bGsHA1u0USqTQk"
    spreadsheet_id = "1q7V_kWXW6-UheYu3s0435atHSlg9wKAHsdp00s6IxhU"

    return client, spreadsheet_id

def createService():
    service = build('sheets', 'v4', credentials=creds)

    return service

def createEntryClient():
    client = gspread.authorize(creds)
    spreadsheet_id = "1kRw7FGZ99v8a16EoOE0L02ltxoo39dgGhJqO2QAZ81g"

    return client, spreadsheet_id
