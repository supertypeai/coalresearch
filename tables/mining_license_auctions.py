from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from scrapper.esdm_minerba import COMMODITY_MAP
from scripts.fuzzy_matcher import match_company_by_name
from datetime import datetime, timedelta, timezone

import logging
import requests
import json
import pandas as pd
import sqlite3
import re
from enum import Enum

logging.basicConfig(
    # no filename → logs go to stderr by default
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

# The API URL
API_URL = "https://minerba.esdm.go.id/lelang/api/pub/lelang_done?page=1"
# DB local
DB_PATH = "db.sqlite"
TIME_OFFSET = timezone(timedelta(hours=7))


class QualificationResult(str, Enum):
    LOLOS = "Lolos"
    TIDAK_LOLOS = "Tidak Lolos"


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
        "Accept": "application/json, text/plain, */*",
        "Authorization": "",
        "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        "Referer": "https://minerba.esdm.go.id/lelang/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    }

    # Initialize the selenium-wire driver
    driver = get_wire_driver()

    try:
        print("Navigating to page to trigger API calls...")
        driver.get("https://minerba.esdm.go.id/lelang/")

        print("Waiting up to 30 seconds for the browser to make the API call...")
        # Use selenium-wire's function to wait for the request
        request = driver.wait_for_request("lelang/api/pub/lelang_done", timeout=30)

        # Grab the Authorization header from the captured request.
        jwt_header = request.headers.get("Authorization")
        if not jwt_header:
            print("No Authorization header found.")
            return None

        headers["Authorization"] = jwt_header
        return headers

    except Exception as error:
        print(
            f"\nFAILED. The browser never made the API call. Likely blocked by bot detection."
        )
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
                LOGGER.info(json.dumps(data.get("data")[1], indent=2))
                return data.get("data", None)

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

def parse_timestamp(timestamp: int) -> str:
    date = datetime.fromtimestamp(timestamp / 1_000_000, tz=TIME_OFFSET)
    return date.strftime("%Y-%m-%d")    

def format_data(
    result_data: list, data: dict, participant: dict, winner_date: str
) -> list[dict]:
    """
    Format the data into a specific structure and append it to result_data.
    This function checks the validity of the input data and formats it accordingly.

    Args:
        result_data (list[dict]): The list to append the formatted data to.
        data (dict): The main data dictionary containing auction information.
        participant (dict): The participant dictionary containing company information.
        winner_date (str): The date when the auction winner was determined.

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
    filtered_tahapan = drop_data_dict(
        data.get("tahapan", []),
        ["id", "tahapanName", "tahapanTanggalMulai", "tahapanAkhirTimestamp", "perubahan"],
    )
    filtered_peserta = drop_data_dict(
        data.get("peserta", []),
        ["id", "lelangId", "perusahaanId", "posisiPenetapanPemenangLelang", "isWinner"],
    )

    # Map tahapan and peserta items to English snake_case
    mapped_tahapan = [
        {
            "order": step["tahapanUrut"],
            "description": step["tahapanDeskripsi"],
            "start_date": (
                parse_timestamp(step["tahapanMulaiTimestamp"]) if step["tahapanMulaiTimestamp"] else None
            ),
            "end_date": (
                parse_timestamp(step["tahapanTanggalAkhir"]) if step["tahapanTanggalAkhir"] else None
            ),
        }
        for step in filtered_tahapan
    ]
    mapped_peserta = [
        {
            "NIB": p["perusahaanNib"],
            "company_name": str(p["perusahaanNama"]).title(),
            "email": p["perusahaanUserEmail"] if p["perusahaanUserEmail"] != "" else None,
            "qualification_result": (
                QualificationResult.LOLOS.value
                if p["hasilAkhirPra"] == "LOLOS"
                else QualificationResult.TIDAK_LOLOS.value
            ),
        }
        for p in filtered_peserta
    ]

    # Format the necessary data
    result_data.append(
        {
            "commodity_type": data.get("komoditas"),
            "city": data.get("namaKab"),
            "province": data.get("namaProv"),
            "company_name": participant.get("perusahaanNama"),
            "winner_date": winner_date,
            "licensed_area_ha": data.get("luasSk"),  # Renamed from luas_sk
            "license_number": data.get("nomor"),  # Renamed from nomor
            "area_type": data.get("jenisIzin"),  # Renamed from jenis_izin
            "kdi": data.get("kdi"),
            "wiup_code": data.get("kodeWiup"),
            "auction_status": data.get("tahapanSaatIni"),
            "created_at": data.get("createdAt"),
            "last_modified": data.get("lastModified"),
            "participant_count": data.get(
                "jumlahPeserta"
            ),  # Renamed from jumlah_peserta
            "phases": mapped_tahapan,  # Renamed from tahapan
            "participants": mapped_peserta,  # Renamed from peserta
            "winner": participant.get("isWinner"),
        }
    )

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
    if not isinstance(result_data, list) or not all(
        isinstance(d, dict) for d in result_data
    ):
        LOGGER.error("Invalid result_data format. Expected a list of dictionaries.")
        return pd.DataFrame()

    df_auction = pd.DataFrame(result_data)

    # Standardize data datetime format for created_at, last_modified, and tahapanMulaiTimestamp
    df_auction["created_at"] = pd.to_datetime(
        df_auction["created_at"], unit="ms"
    ).dt.strftime("%Y-%m-%d")
    df_auction["last_modified"] = pd.to_datetime(
        df_auction["last_modified"],
    ).dt.strftime("%Y-%m-%d")

    # for data_tahapan in df_auction["phase"]:
    #     if isinstance(data_tahapan, list):
    #         for tahapan in data_tahapan:
    #             if "tahapanMulaiTimestamp" in tahapan:
    #                 tahapan["tahapanMulaiTimestamp"] = pd.to_datetime(
    #                     tahapan["tahapanMulaiTimestamp"], unit="ns"
    #                 ).strftime("%Y-%m-%d")

    # Normalized province, city, and company names
    df_auction["province"] = df_auction["province"].str.title().str.strip()
    df_auction["city"] = df_auction["city"].str.title().str.strip()
    df_auction["company_name"] = df_auction["company_name"].str.title().str.strip()

    # Convert to str for winner column
    df_auction["winner"] = df_auction["winner"].astype(str)

    # Normalized commodity
    df_auction["commodity_type"] = df_auction["commodity_type"].map(COMMODITY_MAP)
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
        stage = data.get("tahapanSaatIni")
        commodity = data.get("komoditas")

        if not isinstance(stage, str) or stage.lower() != "lelang selesai":
            continue
        if not isinstance(commodity, str) or commodity.lower() not in [
            "batubara",
            "emas",
            "nikel",
            "tembaga",
        ]:
            continue

        # Loop data key peserta
        for participant in data.get("peserta", []):
            if participant.get("isWinner"):
                winner_date_value = None
                steps = data.get("tahapan", [])

                # Getting date for auction winner
                for step in steps:
                    status_step = step.get("id", "")
                    if status_step.lower().strip() == "penetapanpemenanglelang":
                        # winner_date = step.get("tahapanTanggalMulai")
                        winner_date = step.get("tahapanMulaiTimestamp")
                        
                        if winner_date:   
                            assert isinstance(winner_date, int)
                            winner_date_value = parse_timestamp(winner_date)

                # Prepare data output
                LOGGER.info(
                    f"Processing auction for {commodity} in {data.get('namaKab')}, {data.get('namaProv')}"
                )
                format_data(
                    result_data, data, participant, winner_date_value
                )

    df_cleaned = clean_data(result_data)
    LOGGER.info(f"Total auctions processed: {len(df_cleaned)}")
    return df_cleaned


def sync_company_id(df: pd.DataFrame, target_col: str = "company_name") -> pd.DataFrame:
    df = match_company_by_name(df, target_col, fallback_column=target_col)

    df[target_col] = df[target_col].apply(
        lambda name: (
            None
            if pd.isna(name)
            else re.sub(
                r"\b(PT|Tbk|CV|UD|PD|KSU|KUD)\b", "", str(name), flags=re.IGNORECASE
            ).strip()
        )
    )

    return df


# **************** LOCAL DATABASE SETUP AND PUSH ****************
def create_table(path):
    """
    Create a SQLite database table for mining license auctions.
    This function connects to the SQLite database at the specified path and creates a table
    """
    connection = sqlite3.connect(path)
    # cursor = connection.cursor()
    # cursor.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS mining_license_auctions (
    #         id INTEGER PRIMARY KEY NOT NULL,
    #         commodity_type TEXT,
    #         city TEXT,
    #         province TEXT,
    #         company_name TEXT,
    #         winner_date TEXT,
    #         licensed_area_ha REAL,
    #         license_number TEXT UNIQUE,  --  unique identifier
    #         area_type TEXT,
    #         kdi TEXT,
    #         wiup_code TEXT,
    #         auction_status TEXT,
    #         created_at TEXT,
    #         last_modified TEXT,
    #         participant_count INTEGER,
    #         phases TEXT,
    #         participants TEXT,
    #         winner TEXT,
    #         company_id INTEGER,
    #         FOREIGN KEY (company_id) REFERENCES company(id)
    #     )
    # """
    # )
    # connection.commit()
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
    if isinstance(value, str) and value.strip() == "":
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
    df.insert(0, "id", range(max_id + 1, max_id + 1 + len(df)))
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
        tahapan_json = safe_json_dumps(row["phases"])
        peserta_json = safe_json_dumps(row["participants"])

        data_tuple = (
            row["id"],
            row["commodity_type"],
            row["city"],
            row["province"],
            row["company_name"],
            row["winner_date"],
            row["licensed_area_ha"],
            row["license_number"],
            row["area_type"],
            row["kdi"],
            row["wiup_code"],
            row["auction_status"],
            row["created_at"],
            row["last_modified"],
            row["participant_count"],
            tahapan_json,
            peserta_json,
            row["winner"],
            row["company_id"],
        )
        data_to_insert.append(data_tuple)

    # UPSERT query - using license_number as unique identifier
    # The id field will auto-increment for new records
    upsert_query = """
        INSERT INTO mining_license_auctions (
            id, commodity_type, city, province, company_name, winner_date, 
            licensed_area_ha, license_number, area_type, kdi, wiup_code, 
            auction_status, created_at, last_modified, participant_count,
            phases, participants, winner, company_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(license_number) DO UPDATE SET
            commodity_type = excluded.commodity_type,
            city = excluded.city,
            province = excluded.province,
            company_name = excluded.company_name,
            winner_date = excluded.winner_date,
            licensed_area_ha = excluded.licensed_area_ha,
            area_type = excluded.area_type,
            kdi = excluded.kdi,
            wiup_code = excluded.wiup_code,
            auction_status = excluded.auction_status,
            created_at = excluded.created_at,
            last_modified = excluded.last_modified,
            participant_count = excluded.participant_count,
            phases = excluded.phases,
            participants = excluded.participants,
            winner = excluded.winner,
            company_id = excluded.company_id
    """

    try:
        # Execute the upsert for all records
        cursor.executemany(upsert_query, data_to_insert)
        conn.commit()
        LOGGER.info(
            f"Successfully upserted {len(data_to_insert)} records to mining_license_auctions table"
        )

    except sqlite3.Error as e:
        LOGGER.error(f"Error during upsert operation: {e}")
        conn.rollback()
        raise

    finally:
        cursor.close()


if __name__ == "__main__":
    data = get_data_lelang_json()
    # See structure at datasets/auction_data_sample.json

    df_cleaned = get_specific_data(data)
    df_cleaned = sync_company_id(df_cleaned)
    conn = create_table(DB_PATH)
    check_upsert_local(conn, df_cleaned)
