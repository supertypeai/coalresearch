import requests
import pandas as pd
import json
import time
import random
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import wraps

# Assume constants.py exists and contains the PROVINCE_ID_MAP dictionary
# Example constants.py:
# PROVINCE_ID_MAP = {"11": "ACEH", "12": "SUMATERA UTARA", ...}
import constants

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
CONFIG = {
    "MAX_WORKERS": 16,  # Number of parallel threads. Increase/decrease based on network and server tolerance.
    "CHECKPOINT_INTERVAL": 100,  # Save progress every N companies.
    "RESUME_FROM_CHECKPOINT": True,  # Set to False to start a fresh scrape.
    "BASE_API_URL": "https://minerbaone.esdm.go.id/api/common/v2/publik",
    "COMPANY_LIST_PAGE_SIZE": 10000,  # Number of companies to fetch per API call in the initial phase.
    "RATE_LIMIT_MIN_DELAY": 0.5,  # Minimum delay between requests per thread.
    "RATE_LIMIT_MAX_DELAY": 1.5,  # Maximum delay between requests per thread.
    "RATE_LIMIT_MAX_RETRIES": 5,  # Max retries for a single request if it fails.
}


# ==============================================================================
# --- DECORATORS & SESSION MANAGEMENT ---
# ==============================================================================
def rate_limited_request(
    min_delay=CONFIG["RATE_LIMIT_MIN_DELAY"],
    max_delay=CONFIG["RATE_LIMIT_MAX_DELAY"],
    max_retries=CONFIG["RATE_LIMIT_MAX_RETRIES"],
):
    """Decorator to add intelligent rate limiting and retry logic to HTTP requests."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # The first argument should be the session object, the second should be the URL
            session, url = args[0], args[1]

            for attempt in range(max_retries):
                try:
                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)

                    response = func(*args, **kwargs)
                    response.raise_for_status()  # This will raise an HTTPError for 4xx/5xx responses
                    return response.json()

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:  # Too Many Requests
                        wait_time = (2**attempt) + random.uniform(1, 3)
                        print(
                            f"  [!] Rate limited (429). Thread waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries} for {url}"
                        )
                        time.sleep(wait_time)
                    elif e.response.status_code >= 500:  # Server errors
                        wait_time = (2**attempt) + random.uniform(1, 2)
                        print(
                            f"  [!] Server error ({e.response.status_code}). Thread waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries} for {url}"
                        )
                        time.sleep(wait_time)
                    else:
                        print(
                            f"  [!] HTTP Error {e.response.status_code} for {url}. Giving up."
                        )
                        raise  # Re-raise the exception if it's not a retriable error

                except requests.exceptions.RequestException as e:
                    wait_time = (2**attempt) + random.uniform(1, 2)
                    print(
                        f"  [!] Request exception: {e}. Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries} for {url}"
                    )
                    time.sleep(wait_time)

            raise Exception(f"Max retries ({max_retries}) exceeded for URL: {url}")

        return wrapper

    return decorator


def create_session() -> requests.Session:
    """Creates and configures a requests Session object for efficient network requests."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://minerbaone.esdm.go.id",
            "Referer": "https://minerbaone.esdm.go.id/landing",
        }
    )
    print("✓ Requests session created successfully.")
    return session


# ==============================================================================
# --- DATA FETCHING & PARSING ---
# ==============================================================================
def get_all_company_links(session: requests.Session) -> list:
    """Fetches all company master data from the API."""
    page = 1
    limit = CONFIG["COMPANY_LIST_PAGE_SIZE"]
    url = f"{CONFIG['BASE_API_URL']}/badan-usaha?sort=nama_badan_usaha&page={page}&limit={limit}"

    print(f"Fetching company list from API... (Page size: {limit})")
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()

        company_links = [
            {
                "name": data["nama_badan_usaha"],
                "link": f"https://minerbaone.esdm.go.id/publik/badan-usaha/detail/{data['id_badan_usaha']}",
                "modi_company_id": data["id_badan_usaha"],
            }
            for data in payload.get("data", {}).get("data", [])
        ]

        total_companies = payload.get("data", {}).get("total", len(company_links))
        print(
            f"✓ Successfully fetched {len(company_links)} company links out of {total_companies} total."
        )
        if total_companies > limit:
            print(
                "[!] Warning: Total companies exceeds page size. Pagination logic may be needed in the future."
            )

        return company_links

    except requests.exceptions.RequestException as e:
        print(f"[X] Fatal: Could not fetch company list. Error: {e}")
        return []


@rate_limited_request()
def get_company_profile_v2(session: requests.Session, url: str):
    """Wrapper function for the rate limited API call to get profile."""
    return session.get(url, timeout=20)


@rate_limited_request()
def get_company_mining_licenses_v2(session: requests.Session, url: str):
    """Wrapper function for the rate limited API call to get licenses."""
    return session.get(url, timeout=20)


def extract_company_detail_v2(session: requests.Session, company_id: str) -> dict:
    """Extracts profile and license details for a single company."""
    profile_url = f"{CONFIG['BASE_API_URL']}/badan-usaha/{company_id}"
    licenses_url = f"{profile_url}/list-perizinan"

    final_data = {"url": profile_url, "modi_company_id": company_id}

    try:
        # --- 1. Get Company Profile ---
        profile_payload = get_company_profile_v2(session, profile_url)
        profile_data = profile_payload.get("data", {})
        company_type_map = {
            "4": "CV",
            "2": "PT",
            "6": "SDR",
            "11": "Koperasi",
            "7": "PD",
        }
        company_type_id = str(profile_data.get("id_jenis_badan_usaha"))

        final_data["profil_perusahaan"] = {
            "Nama Perusahaan": profile_data.get("nama_badan_usaha"),
            "Jenis Badan Usaha": company_type_map.get(company_type_id),
        }

        # --- 2. Get Mining Licenses ---
        licenses_payload = get_company_mining_licenses_v2(session, licenses_url)
        licenses_container = licenses_payload.get("data", {})
        licenses_list = licenses_container.get("data", [])

        licenses = []
        for item in licenses_list:
            if not isinstance(item, dict):
                continue

            def safe_get(obj, keys, default=None):
                try:
                    for key in keys:
                        obj = obj[key]
                    return obj
                except (KeyError, TypeError, IndexError):
                    return default

            kabupaten = safe_get(
                item, ["perizinan_kabupaten", 0, "kabupaten", "nama_kabupaten"]
            )
            kode_provinsi = safe_get(
                item, ["perizinan_kabupaten", 0, "kabupaten", "kode_provinsi"]
            )

            licenses.append(
                {
                    "JenisPerizinan": safe_get(
                        item, ["jenis_perizinan", "jenis_perizinan"]
                    ),
                    "NomorPerizinan": item.get("nomor_izin"),
                    "TahapanKegiatan": safe_get(item, ["tahap_kegiatan", "deskripsi"]),
                    "KodeWIUP": safe_get(item, ["wiup", "nomor_wiup"]),
                    "Komoditas": safe_get(item, ["komoditas", "nama_komoditas"]),
                    "Luas(ha)": item.get("luas_ha"),
                    "TglMulaiBerlaku": item.get("tanggal_berlaku"),
                    "TglBerakhir": item.get("tanggal_berakhir"),
                    "Tahapan CNC": safe_get(item, ["status_cnc", "status_cnc"]),
                    "Lokasi": item.get("lokasi_perizinan"),
                    "Kabupaten": kabupaten,
                    "Provinsi": (
                        constants.PROVINCE_ID_MAP.get(kode_provinsi)
                        if kode_provinsi
                        else None
                    ),
                }
            )
        final_data["perizinan_data"] = licenses
        return final_data

    except Exception as error:
        print(f"  [!] Error processing company ID {company_id}: {error}")
        return {"url": profile_url, "modi_company_id": company_id, "error": str(error)}


def process_to_string(scraped_data: dict) -> dict:
    """Converts nested scraped data (lists/dicts) into JSON-formatted strings for CSV."""
    return {
        key: (
            json.dumps(value, ensure_ascii=False, indent=2)
            if isinstance(value, (list, dict))
            else value
        )
        for key, value in scraped_data.items()
    }


# ==============================================================================
# --- CHECKPOINTING & FILE HANDLING ---
# ==============================================================================
def save_checkpoint(data, failed_ids, checkpoint_num):
    """Saves current progress to checkpoint files."""
    checkpoint_dir = "checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if data:
        df = pd.DataFrame(data)
        file_path = f"{checkpoint_dir}/checkpoint_{checkpoint_num}_{timestamp}.csv"
        df.to_csv(file_path, index=False)
        print(
            f"\n✓ Checkpoint {checkpoint_num} saved: {len(data)} records to {file_path}"
        )

    if failed_ids:
        file_path = f"{checkpoint_dir}/failed_ids_{checkpoint_num}_{timestamp}.json"
        with open(file_path, "w") as f:
            json.dump(failed_ids, f, indent=2)
        print(f"✓ Failed IDs saved: {len(failed_ids)} IDs to {file_path}")


def get_processed_company_ids(checkpoint_dir="checkpoints") -> set:
    """Gets a set of already processed company IDs from all checkpoints."""
    processed_ids = set()
    if not os.path.exists(checkpoint_dir):
        return processed_ids

    for filename in os.listdir(checkpoint_dir):
        if filename.startswith("checkpoint_") and filename.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(checkpoint_dir, filename))
                if "modi_company_id" in df.columns:
                    # Drop NA values and convert to string to ensure consistency
                    processed_ids.update(
                        df["modi_company_id"].dropna().astype(str).tolist()
                    )
            except Exception as e:
                print(f"[!] Warning: Could not read checkpoint {filename}: {e}")
    return processed_ids


# ==============================================================================
# --- MAIN EXECUTION LOGIC ---
# ==============================================================================
def run_extract_company_details(
    session: requests.Session, df_links: pd.DataFrame
) -> tuple[pd.DataFrame, list]:
    """Runs concurrent extraction of company details with checkpointing."""

    processed_ids = (
        get_processed_company_ids() if CONFIG["RESUME_FROM_CHECKPOINT"] else set()
    )
    if processed_ids:
        original_count = len(df_links)
        df_links = df_links[~df_links["modi_company_id"].isin(processed_ids)]
        print(
            f"Resuming scrape. Skipping {original_count - len(df_links)} already processed companies."
        )
        print(f"{len(df_links)} new companies remaining to process.")

    if df_links.empty:
        print("No new companies to process. Exiting detail extraction.")
        # Load the last checkpoint to return all data
        all_data_df = pd.concat(
            [
                pd.read_csv(f"checkpoints/{f}")
                for f in os.listdir("checkpoints")
                if f.endswith(".csv")
            ],
            ignore_index=True,
        )
        return all_data_df, []

    all_company_data = []
    failed_company_ids = []

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
        future_to_id = {
            executor.submit(
                extract_company_detail_v2, session, row["modi_company_id"]
            ): row["modi_company_id"]
            for _, row in df_links.iterrows()
        }

        checkpoint_num = (
            len([f for f in os.listdir("checkpoints") if f.startswith("checkpoint_")])
            if os.path.exists("checkpoints")
            else 0
        )

        for future in tqdm(
            as_completed(future_to_id),
            total=len(df_links),
            desc="Scraping Company Details",
        ):
            company_id = future_to_id[future]
            try:
                details_dict = future.result()
                if "error" in details_dict:
                    failed_company_ids.append(company_id)
                else:
                    all_company_data.append(process_to_string(details_dict))

                if (
                    len(all_company_data) % CONFIG["CHECKPOINT_INTERVAL"] == 0
                    and all_company_data
                ):
                    checkpoint_num += 1
                    save_checkpoint(
                        all_company_data, failed_company_ids, checkpoint_num
                    )

            except Exception as exc:
                print(
                    f"  [!] A task for company ID {company_id} generated an unhandled exception: {exc}"
                )
                failed_company_ids.append(company_id)

    if all_company_data:
        print("Saving final checkpoint...")
        checkpoint_num += 1
        save_checkpoint(all_company_data, failed_company_ids, checkpoint_num)

    df_details = pd.DataFrame(all_company_data)
    return df_details, failed_company_ids


if __name__ == "__main__":
    print("--- Starting MODI/MinerbaOne Scraper ---")

    # 1. Create a persistent session for all requests
    session = create_session()

    # 2. Fetch all company links
    company_links_list = get_all_company_links(session)

    if company_links_list:
        df_links = pd.DataFrame(company_links_list)
        df_links["modi_company_id"] = df_links["modi_company_id"].astype(
            str
        )  # Ensure ID is string for matching

        # 3. Scrape details for all companies concurrently
        print(f"\nStarting concurrent scraping with {CONFIG['MAX_WORKERS']} workers...")
        df_new_data, failed_ids = run_extract_company_details(session, df_links)

        # 4. Combine new data with existing checkpoint data
        print("\nCombining all data from checkpoints...")
        all_data_frames = []
        checkpoint_dir = "checkpoints"
        if os.path.exists(checkpoint_dir):
            for filename in os.listdir(checkpoint_dir):
                if filename.startswith("checkpoint_") and filename.endswith(".csv"):
                    all_data_frames.append(
                        pd.read_csv(os.path.join(checkpoint_dir, filename))
                    )

        if not all_data_frames:
            print("No checkpoint data found. Final result is based on this run only.")
            final_df = df_new_data
        else:
            final_df = pd.concat(all_data_frames, ignore_index=True).drop_duplicates(
                subset=["modi_company_id"]
            )

        # 5. Save final results
        os.makedirs("datasets", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if not final_df.empty:
            final_csv_path = f"datasets/modi_company_all_data_v2_{timestamp}.csv"
            final_df.to_csv(final_csv_path, index=False)
            print(f"✓ Final combined results saved to: {final_csv_path}")

        if failed_ids:
            final_failed_path = f"datasets/final_failed_ids_{timestamp}.json"
            with open(final_failed_path, "w") as f:
                json.dump(failed_ids, f, indent=2)
            print(f"✓ Final list of failed IDs saved to: {final_failed_path}")

        print("\n--- Scraping process completed. ---")
        print(f"Total unique companies saved: {len(final_df)}")
        print(f"Number of failed company IDs in the last run: {len(failed_ids)}")
    else:
        print("\n[X] Could not retrieve any company links. Exiting.")
