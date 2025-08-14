from seleniumwire                       import webdriver
from selenium.webdriver.chrome.service  import Service
from webdriver_manager.chrome           import ChromeDriverManager

from scrapper.esdm_minerba  import COMMODITY_MAP
from scripts.fuzzy_matcher import match_company_by_name

import logging
import requests
import json
import pandas as pd 
import sqlite3 

logging.basicConfig(
    # no filename → logs go to stderr by default
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

# The API URL 
API_URL = "https://minerba.esdm.go.id/lelang/api/pub/lelang_done?page=1"
# DB local
DB_PATH = 'db.sqlite'


def get_wire_driver(is_headless: bool = True) -> webdriver.Chrome:
    """
    Initializes a selenium-wire WebDriver.

    Args:
        is_headless (bool): If True, runs the browser in headless mode. Default is True.
    
    Returns:
        webdriver.Chrome: An instance of the Chrome WebDriver configured with selenium-wire.
    """
    options = webdriver.ChromeOptions()
    if is_headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_jwt_auth() -> dict[str]:
    """ 
    Get the JWT Authorization header from the ESDM Minerba API.
    This function uses Selenium Wire to capture the Authorization header from the API call.

    Returns:
        dict: A dictionary containing the Authorization header for the API request.
    """
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': "",
        'Accept-Language': 'en-US,en;q=0.9,id;q=0.8', 
        'Referer': 'https://minerba.esdm.go.id/lelang/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36', 
    }
    
    # Initialize the selenium-wire driver
    driver = get_wire_driver()

    try:
        print("Navigating to page to trigger API calls...")
        driver.get("https://minerba.esdm.go.id/lelang/")

        print("Waiting up to 30 seconds for the browser to make the API call...")
        # Use selenium-wire's function to wait for the request
        request = driver.wait_for_request(
            'lelang/api/pub/lelang_done',
            timeout=30
        )

        # Grab the Authorization header from the captured request.
        jwt_header = request.headers.get('Authorization')
        if not jwt_header:
            print("No Authorization header found.")
            return None
        
        headers["Authorization"] = jwt_header
        return headers
    
    except Exception as error:
        print(f"\nFAILED. The browser never made the API call. Likely blocked by bot detection.")
        print(f"Error: {error}")

    finally:
        if driver:
            print("\nClosing the driver.")
            driver.quit()


def get_data_lelang_json() -> list[dict]:
    """ 
    Fetch data from the ESDM Minerba API and return it as a JSON object.
    This function handles the request, checks the response status, and returns the data.
    """
    try:
        LOGGER.info(f"Sending request to: {API_URL}")
        # Make the request to the API, now including the headers
        headers = get_jwt_auth()
        response = requests.get(API_URL, headers=headers)

        # Check the status code from the server's response
        LOGGER.info(f"Received status code: {response.status_code}")

        # Check if the request was successful
        if response.status_code == 200:
            LOGGER.info("Success! Data received.")
            # Get the JSON data from the response
            data = response.json()
            if data:
                # inspect one data
                LOGGER.info(json.dumps(data.get('data')[1], indent=2))
                return data.get('data', None)

        else:
            LOGGER.info("Request failed. Server response:")
            LOGGER.info(response.text)

    except requests.exceptions.RequestException as e:
        LOGGER.info(f"An error occurred with the request: {e}")


def drop_data_dict(datas: list[dict], columns_to_filter: list[str]) -> list[dict]: 
    """
    Iterate over each dict in `datas`, remove any keys in `columns_to_filter`,
    and return a new list of cleaned dicts.
    
    Args:
        datas (list[dict]): List of dictionaries to filter.
        columns_to_filter (list[str]): List of keys to remove from each dictionary. 
        
    Returns:
        list[dict]: A new list of dictionaries with the specified keys removed.
    """
    if not isinstance(datas, list) or not all(isinstance(d, dict) for d in datas):
        LOGGER.error("Invalid data format. Expected a list of dictionaries.")
        return []
    
    filtered_list = []
    for data in datas: 
        # Build a new dict without the unwanted keys
        cleaned = {}
        for key, value in data.items(): 
            if key not in columns_to_filter:
                cleaned[key] = value
        filtered_list.append(cleaned)
    return filtered_list


def format_data(result_data: list, 
                data: dict, 
                participant: dict, 
                date_winner: str) -> list[dict]: 
    """
    Format the data into a specific structure and append it to result_data.
    This function checks the validity of the input data and formats it accordingly.
    
    Args:
        result_data (list[dict]): The list to append the formatted data to.
        data (dict): The main data dictionary containing auction information.
        participant (dict): The participant dictionary containing company information.
        date_winner (str): The date when the auction winner was determined.
        
    Returns:
        list[dict]: The updated result_data with the formatted entry appended.
    """
    # Check if the input data is valid
    if not isinstance(data, dict) or not isinstance(participant, dict):
        LOGGER.error("Invalid data format. Expected dictionaries.")
        return result_data
    
    if not isinstance(result_data, list):
        LOGGER.error("Result data should be a list.")
        return result_data
    
    # Drop unwanted keys from the tahapan and peserta data
    filtered_tahapan = drop_data_dict(data.get('tahapan', []), ['id', 'tahapanName', 
                                                                'tahapanAkhirTimestamp', 
                                                                'perubahan'])
    filtered_peserta = drop_data_dict(data.get('peserta', []), ['id', 'lelangId', 
                                                                'perusahaanId', 
                                                                'posisiPenetapanPemenangLelang',
                                                                'isWinner'])
    
    # Format the necessary data
    result_data.append({
                    'commodity': data.get('komoditas'),
                    'city': data.get('namaKab'),
                    'province': data.get('namaProv'),
                    'company_name': participant.get('perusahaanNama'),
                    'date_winner': date_winner,
                    'luas_sk': data.get('luasSk'),
                    'nomor': data.get('nomor'),
                    'jenis_izin': data.get('jenisIzin'),
                    'kdi': data.get('kdi'), 
                    'code_wiup': data.get('kodeWiup'), 
                    'auction_status': data.get('tahapanSaatIni'),
                    'created_at': data.get('createdAt'), 
                    'last_modified': data.get('lastModified'), 
                    'jumlah_peserta': data.get('jumlahPeserta'), 
                    'tahapan': filtered_tahapan,
                    'peserta': filtered_peserta, 
                    'winner': participant.get('isWinner')
                })
    
    return result_data


def clean_data(result_data: list[dict]) -> pd.DataFrame: 
    """ 
    Clean and standardize the data in result_data.
    This function converts timestamps to a standard format, normalizes province and city names,
    
    Args:
        result_data (list[dict]): The list of dictionaries containing auction data.
    
    Returns:
        pd.DataFrame: A cleaned DataFrame with standardized date formats and normalized names.
    """
    # Check if result_data is a list of dictionaries
    if not isinstance(result_data, list) or not all(isinstance(d, dict) for d in result_data):
        LOGGER.error("Invalid result_data format. Expected a list of dictionaries.")
        return []
    
    df_auction = pd.DataFrame(result_data) 
    
    # Standardize data datetime format for created_at, last_modified, and tahapanMulaiTimestamp
    df_auction['created_at'] = pd.to_datetime(
                                    df_auction['created_at'], 
                                    unit='ms'
                                ).dt.strftime('%Y-%m-%d')
    df_auction['last_modified'] = pd.to_datetime(
                                    df_auction['last_modified'], 
                                ).dt.strftime('%Y-%m-%d')
    
    for data_tahapan in df_auction['tahapan']:
        if isinstance(data_tahapan, list):
            for tahapan in data_tahapan:
                if 'tahapanMulaiTimestamp' in tahapan:
                    tahapan['tahapanMulaiTimestamp'] = pd.to_datetime(
                        tahapan['tahapanMulaiTimestamp'], 
                        unit='ns'
                    ).strftime('%Y-%m-%d')
    
    # Normalized province, city, and company names                
    df_auction['province'] = df_auction['province'].str.title().str.strip()
    df_auction['city'] = df_auction['city'].str.title().str.strip()
    df_auction['company_name'] = df_auction['company_name'].str.title().str.strip()
    
    # Convert to str for winner column
    df_auction['winner'] = df_auction['winner'].astype(str)
    
    # Normalized commodity 
    df_auction['commodity'] = df_auction['commodity'].map(COMMODITY_MAP)
    return df_auction
    
    
def get_specific_data(data_json: list[dict]) -> pd.DataFrame: 
    """ 
    Extract specific data from the JSON response.
    This function filters the data to include only completed auctions for specific commodities
    (coal, gold, nikel, tembaga) and formats the data.
    
    Args:
        data_json (dict): The JSON data containing auction information.
    
    Returns:
        pd.DataFrame: A DataFrame containing the cleaned and formatted auction data.
    """
    # Loop data json
    result_data = []
    for data in data_json:
        if not isinstance(data, dict):
            continue
        
        # Get only data with stage is lelang selesai
        stage = data.get('tahapanSaatIni')
        commodity = data.get('komoditas')
        
        if not isinstance(stage, str) or stage.lower() != "lelang selesai":
            continue
        if not isinstance(commodity, str) or commodity.lower() not in ['batubara', 'emas', 
                                                                       'nikel', 'tembaga']:
            continue
        
        # Loop data key peserta 
        for participant in data.get('peserta', []):
            if participant.get('isWinner'):
                date_winner_value = None
                steps = data.get('tahapan', [])
                
                # Getting date for auction winner
                for step in steps: 
                    status_step = step.get('id', '')
                    if status_step.lower().strip() == 'penetapanpemenanglelang':
                        date_winner = step.get('tahapanTanggalMulai')
                        if date_winner:
                            date_winner_value = date_winner 
                
                # Prepare data output
                LOGGER.info(f"Processing auction for {commodity} in {data.get('namaKab')}, {data.get('namaProv')}")
                data_formatted = format_data(result_data, 
                            data, 
                            participant, 
                            date_winner_value)
    
    df_cleaned = clean_data(data_formatted)
    LOGGER.info(f"Total auctions processed: {len(df_cleaned)}")
    return df_cleaned


# **************** LOCAL DATABASE SETUP AND PUSH ****************
def create_table(path): 
    """ 
    Create a SQLite database table for mining license auctions.
    This function connects to the SQLite database at the specified path and creates a table
    """
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mining_license_auctions (
            id INTEGER PRIMARY KEY NOT NULL,
            commodity TEXT,
            city TEXT,
            province TEXT,
            company_name TEXT,
            date_winner TEXT,
            luas_sk REAL,
            nomor TEXT UNIQUE,  --  unique identifier
            jenis_izin TEXT,
            kdi TEXT,
            code_wiup TEXT,
            auction_status TEXT,
            created_at TEXT,
            last_modified TEXT,
            jumlah_peserta INTEGER,
            tahapan TEXT,
            peserta TEXT,
            winner TEXT,
            company_id INTEGER,
            FOREIGN KEY (company_id) REFERENCES company(id)
        )
    ''')
    connection.commit()
    return connection


def safe_json_dumps(value: any) -> str:
    """ 
    Safely convert a value to a JSON string, handling None and empty values.
    This function checks if the value is None or an empty string, and returns None in those
    cases. If the value is a list or dictionary, it converts it to a JSON string.
    Otherwise, it converts the value to a JSON string.

    Args:
        value (any): The value to convert to a JSON string.
    
    Returns:
        str: A JSON string representation of the value, or None if the value is None or
    """
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == '':
        return None
    if isinstance(value, (list, dict)):
        # Empty list or dict
        if not value:  
            return None
        return json.dumps(value)
    return json.dumps(value)


def prepare_id(conn: sqlite3.Connection, df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Assign sequential integer IDs to a DataFrame based on the current maximum `id` in the database.

    Args: 
        conn  (sqlite3.Connection): A connection to the SQLite database 
        df (pandas.DataFrame): A DataFrame of new rows to be inserted.
    
    Returns:
        pd.DataFrame: A copy of `df` with a new integer `id` column prepended.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM mining_license_auctions")
    max_id = cursor.fetchone()[0] or 0
    cursor.close()

    df = df.copy()
    df.insert(0, 'id', range(max_id + 1, max_id + 1 + len(df)))
    return df


def check_upsert_local(conn: sqlite3.Connection, df: pd.DataFrame):
    """
    Insert or update data in the lelang_minerba table.
    Uses UPSERT operation to handle existing records.
    Uses 'nomor' as the business unique identifier.

    Args:
        conn (sqlite3.Connection): The SQLite connection object.
        df (pd.DataFrame): The DataFrame containing the auction data to be inserted or updated.
    """
    cursor = conn.cursor()
    df = prepare_id(conn, df)

    # Convert DataFrame to list of tuples for bulk insert
    data_to_insert = []
    
    for _, row in df.iterrows():
        # Convert complex data types to JSON strings
        tahapan_json = safe_json_dumps(row['tahapan'])
        peserta_json = safe_json_dumps(row['peserta'])
        
        data_tuple = (
            row['id'],
            row['commodity'],
            row['city'], 
            row['province'],
            row['company_name'],
            row['date_winner'],
            row['luas_sk'],
            row['nomor'],
            row['jenis_izin'],
            row['kdi'],
            row['code_wiup'],
            row['auction_status'],
            row['created_at'],
            row['last_modified'],
            row['jumlah_peserta'],
            tahapan_json,
            peserta_json,
            row['winner'],
            row['company_id']
        )
        data_to_insert.append(data_tuple)
    
    # UPSERT query - using nomor as unique identifier
    # The id field will auto-increment for new records
    upsert_query = """
        INSERT INTO mining_license_auctions (
            id, commodity, city, province, company_name, date_winner, 
            luas_sk, nomor, jenis_izin, kdi, code_wiup, 
            auction_status, created_at, last_modified, jumlah_peserta,
            tahapan, peserta, winner, company_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(nomor) DO UPDATE SET
            commodity = excluded.commodity,
            city = excluded.city,
            province = excluded.province,
            company_name = excluded.company_name,
            date_winner = excluded.date_winner,
            luas_sk = excluded.luas_sk,
            jenis_izin = excluded.jenis_izin,
            kdi = excluded.kdi,
            code_wiup = excluded.code_wiup,
            auction_status = excluded.auction_status,
            created_at = excluded.created_at,
            last_modified = excluded.last_modified,
            jumlah_peserta = excluded.jumlah_peserta,
            tahapan = excluded.tahapan,
            peserta = excluded.peserta,
            winner = excluded.winner,
            company_id = excluded.company_id
    """
    
    try:
        # Execute the upsert for all records
        cursor.executemany(upsert_query, data_to_insert)
        conn.commit()
        LOGGER.info(f"Successfully upserted {len(data_to_insert)} records to mining_license_auctions table")
        
    except sqlite3.Error as e:
        LOGGER.error(f"Error during upsert operation: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()
    

if __name__ == '__main__':
    data = get_data_lelang_json()
    df_cleaned = get_specific_data(data)
    df_cleaned = match_company_by_name(df_cleaned, "company_name")
    conn = create_table(DB_PATH)
    check_upsert_local(conn, df_cleaned)
   