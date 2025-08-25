from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

import gspread
import os

load_dotenv()

SERVICE_ACCOUNT_DIR = os.getenv('SERVICE_ACC_DIR', '')

if not SERVICE_ACCOUNT_DIR:
    print("Service account key is not available, exiting!")
    exit(1)

# Define scope
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_DIR, scopes=SCOPES)

def createClient(): 
    client = gspread.authorize(creds)
    spreadsheet_id = "19wfJ2fc9qKeR22dMIO2rEQLkit8E4bGsHA1u0USqTQk"

    return client, spreadsheet_id

def createService():
    service = build('sheets', 'v4', credentials=creds)

    return service

def createEntryClient():
    client = gspread.authorize(creds)
    spreadsheet_id = "1kRw7FGZ99v8a16EoOE0L02ltxoo39dgGhJqO2QAZ81g"

    return client, spreadsheet_id
