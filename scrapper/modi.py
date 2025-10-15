import requests
import pandas as pd
import json
import time
import random
import os
from datetime import datetime

import constants

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import wraps


def save_checkpoint(data, failed_urls, checkpoint_num, timestamp=None):
    """Save current progress to checkpoint files"""
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    checkpoint_dir = "checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Save successful data
    if data:
        df = pd.DataFrame(data)
        checkpoint_file = f"{checkpoint_dir}/checkpoint_{checkpoint_num}_{timestamp}.csv"
        df.to_csv(checkpoint_file, index=False)
        print(f"✓ Checkpoint {checkpoint_num} saved: {len(data)} records to {checkpoint_file}")
    
    # Save failed URLs
    if failed_urls:
        failed_file = f"{checkpoint_dir}/failed_urls_{checkpoint_num}_{timestamp}.json"
        with open(failed_file, 'w') as f:
            json.dump(failed_urls, f, indent=2)
        print(f"✓ Failed URLs saved: {len(failed_urls)} URLs to {failed_file}")
    
    return timestamp


def load_checkpoint(checkpoint_dir="checkpoints"):
    """Load the most recent checkpoint if available"""
    if not os.path.exists(checkpoint_dir):
        return [], [], 0
    
    # Find the most recent checkpoint
    checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith("checkpoint_") and f.endswith(".csv")]
    if not checkpoint_files:
        return [], [], 0
    
    # Sort by timestamp and get the latest
    latest_file = sorted(checkpoint_files)[-1]
    checkpoint_path = os.path.join(checkpoint_dir, latest_file)
    
    # Extract checkpoint number from filename
    try:
        checkpoint_num = int(latest_file.split("_")[1])
    except (IndexError, ValueError):
        checkpoint_num = 0
    
    # Load data
    df = pd.read_csv(checkpoint_path)
    data = df.to_dict('records')
    
    # Load failed URLs if they exist
    failed_file = latest_file.replace("checkpoint_", "failed_urls_").replace(".csv", ".json")
    failed_path = os.path.join(checkpoint_dir, failed_file)
    failed_urls = []
    if os.path.exists(failed_path):
        with open(failed_path, 'r') as f:
            failed_urls = json.load(f)
    
    print(f"✓ Loaded checkpoint {checkpoint_num}: {len(data)} records, {len(failed_urls)} failed URLs")
    return data, failed_urls, checkpoint_num


def get_processed_company_ids(checkpoint_dir="checkpoints"):
    """Get list of already processed company IDs from all checkpoints"""
    processed_ids = set()
    
    if not os.path.exists(checkpoint_dir):
        return processed_ids
    
    # Check all checkpoint files
    checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith("checkpoint_") and f.endswith(".csv")]
    
    for checkpoint_file in checkpoint_files:
        try:
            checkpoint_path = os.path.join(checkpoint_dir, checkpoint_file)
            df = pd.read_csv(checkpoint_path)
            
            # Extract company IDs from URLs
            if 'url' in df.columns:
                for url in df['url']:
                    if isinstance(url, str) and 'badan-usaha/' in url:
                        company_id = url.split('badan-usaha/')[-1]
                        processed_ids.add(company_id)
        except Exception as e:
            print(f"Warning: Could not read checkpoint {checkpoint_file}: {e}")
    
    return processed_ids


def rate_limited_request(min_delay=0.5, max_delay=2.0, max_retries=3):
    """Decorator to add rate limiting and retry logic to HTTP requests"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    # Add random delay before each request
                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)
                    
                    result = func(*args, **kwargs)
                    
                    # Check if we got rate limited
                    if hasattr(result, 'status_code'):
                        if result.status_code == 429:  # Too Many Requests
                            wait_time = (2 ** attempt) + random.uniform(1, 3)
                            print(f"Rate limited (429). Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries}")
                            time.sleep(wait_time)
                            continue
                        elif result.status_code >= 500:  # Server errors
                            wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                            print(f"Server error ({result.status_code}). Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries}")
                            time.sleep(wait_time)
                            continue
                    
                    return result
                    
                except requests.exceptions.RequestException as e:
                    if "429" in str(e) or "rate limit" in str(e).lower():
                        wait_time = (2 ** attempt) + random.uniform(1, 3)
                        print(f"Rate limit exception. Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries}")
                        time.sleep(wait_time)
                    elif attempt == max_retries - 1:
                        raise
                    else:
                        wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                        print(f"Request exception: {e}. Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries}")
                        time.sleep(wait_time)
            
            raise Exception(f"Max retries ({max_retries}) exceeded")
        return wrapper
    return decorator


def initSession():
    """Initializes a session to get necessary cookies and prepares headers for subsequent requests."""
    url_initial = "https://modi.esdm.go.id/portal/dataPerusahaan"
    headers_initial = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
    }

    try:
        resp_initial = requests.get(url_initial, headers=headers_initial, timeout=30)
        resp_initial.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not initialize session. Error: {e}")
        return None

    cookies = resp_initial.cookies.get_dict()
    cookie_string = "; ".join([f"{key}={value}" for key, value in cookies.items()])
    print("Session initialized. Cookie header string:", cookie_string)

    headers_second = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Host": "modi.esdm.go.id",
        "Referer": "https://modi.esdm.go.id/portal/dataPerusahaan",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": headers_initial["User-Agent"],
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Cookie": cookie_string,
    }

    return headers_second


def accessPage(page: int, headers: dict, limit=10) -> requests.Response:
    """Accesses a single listing page of companies."""
    url_second = f"https://modi.esdm.go.id/portal/dataPerusahaan/getdata?page={page}&sortby=id&sorttype=asc&perusahaan=&noakte="
    url_third = f"https://minerbaone.esdm.go.id/api/common/v2/publik/badan-usaha?sort=nama_badan_usaha&filter[nama_badan_usaha][like]=&page={page}&limit={limit}"
    return requests.get(url_third, timeout=20)


# ----------------------------------------------------------------------------------
# REVISION 1: NEW FUNCTION TO REPLACE `downloadAllPage` and `generateCompanyData`
# ----------------------------------------------------------------------------------
def get_all_company_links(headers: dict) -> list:
    """
    Fetches all listing pages, extracts company links in memory, and returns them as a list.
    This function streamlines the process by removing the intermediate step of saving HTML files.
    """
    first_pg = 1
    last_pg = 1  # with 10000 companies per page
    company_per_page = 10000
    all_company_links = []

    print(f"Fetching company links from {first_pg} to {last_pg} pages...")
    for i in tqdm(range(first_pg, last_pg + 1), desc="Fetching Company List Pages"):
        # try:
            page_response = accessPage(i, headers, limit=company_per_page)
            page_response.raise_for_status()  # Check for HTTP errors

            # Directly parse the response text and extract links
            # links_on_page = extractCompanyLink(page_response.text)
            links_on_page = generate_company_page_link(page_response.json())
            all_company_links.extend(links_on_page)

            # Small delay to be polite to the server
            time.sleep(1)

        # except requests.exceptions.RequestException as e:
        #     print(f"Warning: Failed to fetch page {i}. Error: {e}. Skipping...")
        #     continue

    print(f"Successfully extracted {len(all_company_links)} company links.")
    return all_company_links

def generate_company_page_link(payload):
    return [
        {
            "name": data['nama_badan_usaha'],
            "link": f"https://minerbaone.esdm.go.id/publik/badan-usaha/detail/{data['id_badan_usaha']}",
            "modi_company_id": data['id_badan_usaha']
        }
        for data in payload['data']['data']
    ]

def extractCompanyLink(html: str) -> list:
    # Unused, since they changed the frontend
    """Extracts company names and profile links from the HTML of a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/portal/detailPerusahaan/" in href:
            name = a_tag.text.strip()
            results.append({"name": name, "link": href})
    return results


def parse_html_table(table_element: BeautifulSoup) -> list:
    """Parses an HTML table element into a list of dictionaries using table headers as keys."""
    if not table_element or not table_element.find("thead"):
        return []

    headers = [th.text.strip() for th in table_element.find("thead").find_all("th")]
    rows = []
    for tr in table_element.find("tbody").find_all("tr"):
        cells = [td.text.strip() for td in tr.find_all("td")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    return rows


def get_profil_perusahaan(soup: BeautifulSoup) -> dict:
    """Extracts structured company profile information from a company's profile page."""
    profile_container = soup.find("div", id="profile")
    if not profile_container:
        return {"error": "Profile tab container not found."}

    data = {}
    profil_table = profile_container.find("table")
    profil_data = {}
    if profil_table:
        for row in profil_table.find("tbody").find_all("tr"):
            key_element = row.find("th")
            value_elements = row.find_all("td")
            if key_element and value_elements:
                key = key_element.text.strip()
                value = value_elements[-1].text.strip()
                profil_data[key] = value
    data["profil_perusahaan"] = profil_data

    sections = profile_container.find_all("div", class_="row")
    for section in sections:
        title_tag = section.find("b")
        if not title_tag:
            continue

        title_text = (
            title_tag.text.strip()
            .split(". ")[-1]
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
        )
        if title_text == "pemilik___pemegang_saham":
            title_text = "pemilik_pemegang_saham"

        table = section.find("table")
        if table:
            table_data = parse_html_table(table)
            if table_data:
                data[title_text] = table_data
    return data


def get_alamat(soup: BeautifulSoup) -> dict:
    """Extracts address history from the 'Alamat' section of a company's HTML profile."""
    alamat_container = soup.find("div", id="alamat")
    if not alamat_container:
        return {"error": "Alamat tab container not found."}

    data = {}
    timeline_items = alamat_container.find_all("div", class_="timeline-item")
    history = []
    for item in timeline_items:
        table = item.find("table")
        if table:
            details = parse_html_table(table)
            if details:
                entry = {}
                title_tag = item.find("h5")
                entry["title"] = (
                    title_tag.text.strip() if title_tag else "Alamat Historis"
                )
                entry["details"] = details
                history.append(entry)
    if history:
        data["alamat_history"] = history
    return data


def get_direksi(soup: BeautifulSoup) -> dict:
    """Extracts historical board of directors (direksi) information from the HTML document."""
    direksi_container = soup.find("div", id="direksi")
    if not direksi_container:
        return {"error": "Direksi tab container not found."}

    data = {}
    timeline_items = direksi_container.find_all("div", class_="timeline-item")
    history = []
    for item in timeline_items:
        table = item.find("table")
        if table:
            details = parse_html_table(table)
            if details:
                entry = {}
                title_tag = item.find("h5")
                entry["title"] = (
                    title_tag.text.strip() if title_tag else "Direksi Historis"
                )
                entry["details"] = details
                history.append(entry)
    if history:
        data["direksi_history"] = history
    return data


def get_perizinan(soup: BeautifulSoup) -> dict:
    """Extracts business license (perizinan) data from the HTML document."""
    perizinan_container = soup.find("div", id="perizinan")
    if not perizinan_container:
        return {"error": "Perizinan tab container not found."}

    data = {}
    table = perizinan_container.find("table", id="dt_basics")
    if table:
        table_data = parse_html_table(table)
        if table_data:
            data["perizinan_data"] = table_data
    return data


def process_to_string(scraped_data: dict) -> dict:
    """Converts nested scraped data (lists/dicts) into JSON-formatted strings."""
    processed_row = {}
    for key, value in scraped_data.items():
        if isinstance(value, (list, dict)) and value:
            processed_row[key] = json.dumps(value, ensure_ascii=False, indent=2)
        else:
            processed_row[key] = value
    return processed_row


def extract_company_detail(url: str, headers: str) -> dict:
    """Extracts detailed company information from a given URL using multiple HTML parsers."""
    try:
        # response = requests.get(url, headers=headers, timeout=20)
        print(url)
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        print(response.json())
        parsed_soup = BeautifulSoup(response.text, "html.parser")
        time.sleep(0.35)
    except requests.exceptions.RequestException as error:
        print(f"Could not fetch {url}. Error: {error}")
        return {"url": url, "error": str(error)}

    print(parsed_soup)

    data_profile = get_profil_perusahaan(parsed_soup)
    data_alamat = get_alamat(parsed_soup)
    data_direksi = get_direksi(parsed_soup)
    data_perizinan = get_perizinan(parsed_soup)

    company_data = {**data_profile, **data_alamat, **data_direksi, **data_perizinan}
    company_data["url"] = url
    return company_data

@rate_limited_request(min_delay=1.5, max_delay=3.0, max_retries=5)
def get_company_profile_v2(url: str):
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    
    # Better null checking
    if not payload or not isinstance(payload, dict):
        raise ValueError("Invalid API response: payload is None or not a dict")
    
    data = payload.get('data')
    if not data or not isinstance(data, dict):
        raise ValueError("Invalid API response: data field is None or not a dict")

    company_name = data.get('nama_badan_usaha')
    company_type_id = data.get('id_jenis_badan_usaha')
    company_type_map = {
        "4": "CV", 
        "2": "PT", 
        "6": "SDR", 
        "11": "Koperasi",
        "7": "PD"
    }
    company_type = company_type_map.get(company_type_id) if company_type_id else None

    return {
        "Nama Perusahaan": company_name,
        "Jenis Badan Usaha": company_type
    }

@rate_limited_request(min_delay=1.5, max_delay=3.0, max_retries=5)
def get_company_mining_licenses_v2(url: str):
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    
    # Better null checking
    if not payload or not isinstance(payload, dict):
        raise ValueError("Invalid API response: payload is None or not a dict")
    
    # Handle the nested data structure - the actual licenses are in data.data
    data_container = payload.get('data')
    if not data_container or not isinstance(data_container, dict):
        return []  # No licenses found
    
    data_list = data_container.get('data', [])
    if not isinstance(data_list, list):
        return []
    
    if not data_list:
        return []
    
    def safe_get(obj, keys, default=None):
        """Safely access nested dictionary/list structures."""
        try:
            for key in keys:
                obj = obj[key]
            return obj
        except (KeyError, TypeError, IndexError):
            return default

    # Process each license in the list
    licenses = []
    for item in data_list:
        if not isinstance(item, dict):
            continue
            
        jenis_perizinan_value = safe_get(item, ['jenis_perizinan', 'jenis_perizinan'])
        tahap_kegiatan_value = safe_get(item, ['tahap_kegiatan', 'deskripsi'])
        wiup_value = safe_get(item, ['wiup', 'nomor_wiup'])
        komoditas_value = safe_get(item, ['komoditas', 'nama_komoditas'])
        status_cnc_value = safe_get(item, ['status_cnc', 'status_cnc'])
        kabupaten = safe_get(item, ['perizinan_kabupaten', 0, 'kabupaten', 'nama_kabupaten'])
        kode_provinsi = safe_get(item, ['perizinan_kabupaten', 0, 'kabupaten', 'kode_provinsi'])
        
        license_data = {
            "JenisPerizinan": jenis_perizinan_value,
            "NomorPerizinan": item.get('nomor_izin'),
            "TahapanKegiatan": tahap_kegiatan_value,
            "KodeWIUP": wiup_value,
            "Komoditas": komoditas_value,
            "Luas(ha)": item.get('luas_ha'),
            "TglMulaiBerlaku": item.get('tanggal_berlaku'),
            "TglBerakhir": item.get('tanggal_berakhir'),
            "Tahapan CNC": status_cnc_value,
            "Lokasi": item.get('lokasi_perizinan'),
            "Kabupaten": kabupaten,
            "Provinsi": constants.PROVINCE_ID_MAP.get(kode_provinsi) if kode_provinsi else None,
        }
        licenses.append(license_data)
    return licenses

def extract_company_detail_v2(company_id: str):
    url = f"https://minerbaone.esdm.go.id/api/common/v2/publik/badan-usaha/{company_id}"
    try:
        # Get company profile (with rate limiting built-in)
        company_profile = get_company_profile_v2(url)
        
        # Add a longer delay between the two API calls to the same domain
        time.sleep(random.uniform(0.8, 1.5))
        
        # Get mining licenses (with rate limiting built-in)
        mining_licenses = get_company_mining_licenses_v2(url + "/list-perizinan?")
        
        return {
            "profil_perusahaan": company_profile,
            "perizinan_data": mining_licenses,
            "url": url
        }

    except requests.exceptions.RequestException as error:
        print(f"Request error for company {company_id}: {error}")
        return {"url": url, "error": str(error)}
    except Exception as error:
        print(f"Unexpected error for company {company_id}: {error}")
        return {"url": url, "error": str(error)}

def run_extract_company_details(
    headers: str, df_links: pd.DataFrame
) -> tuple[pd.DataFrame, list]:
    """Runs concurrent extraction of company details using a pool of threads."""
    all_company_data = []
    failed_articles = []
    MAX_WORKERS = 10

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {
            executor.submit(extract_company_detail, row["link"], headers): row["link"]
            for _, row in df_links.iterrows()
        }

        for future in tqdm(
            as_completed(future_to_url),
            total=len(df_links),
            desc="Scraping Company Details",
        ):
            url = future_to_url[future]
            try:
                details_dict = future.result()
                if "error" in details_dict:
                    print(f"Appending url {url} to failed urls")
                    failed_articles.append(url)
                dataframe_row = process_to_string(details_dict)
                all_company_data.append(dataframe_row)
            except Exception as exc:
                url = future_to_url[future]
                print(f"{url} generated an exception: {exc}")
                failed_articles.append(url)

    df_details = pd.DataFrame(all_company_data)
    return df_details, failed_articles

def run_extract_company_details_v2(
    df_links: pd.DataFrame, 
    checkpoint_interval: int = 100,
    resume_from_checkpoint: bool = True
) -> tuple[pd.DataFrame, list]:
    """Runs concurrent extraction of company details using a pool of threads with rate limiting and checkpointing."""
    
    # Load existing checkpoint if resuming
    if resume_from_checkpoint:
        existing_data, existing_failed, last_checkpoint_num = load_checkpoint()
        processed_ids = get_processed_company_ids()
        
        # Filter out already processed companies
        if processed_ids:
            original_count = len(df_links)
            df_links = df_links[~df_links['modi_company_id'].isin(processed_ids)]
            print(f"Resuming: Skipping {original_count - len(df_links)} already processed companies")
            print(f"Remaining to process: {len(df_links)} companies")
    else:
        existing_data, existing_failed, last_checkpoint_num = [], [], 0
    
    all_company_data = existing_data.copy() if existing_data else []
    failed_articles = existing_failed.copy() if existing_failed else []
    MAX_WORKERS = 1  # Reduced to 1 worker to avoid rate limiting completely
    
    if len(df_links) == 0:
        print("No new companies to process!")
        return pd.DataFrame(all_company_data), failed_articles

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {
            executor.submit(extract_company_detail_v2, row["modi_company_id"]): row["modi_company_id"]
            for _, row in df_links.iterrows()
        }

        completed_count = 0
        checkpoint_num = last_checkpoint_num
        
        for future in tqdm(
            as_completed(future_to_url),
            total=len(df_links),
            desc="Scraping Company Details",
        ):
            url = future_to_url[future]
            try:
                details_dict = future.result()
                if "error" in details_dict:
                    print(f"Appending url {url} to failed urls")
                    failed_articles.append(url)
                else:
                    dataframe_row = process_to_string(details_dict)
                    all_company_data.append(dataframe_row)
                
                completed_count += 1
                
                # Save checkpoint every checkpoint_interval completions
                if completed_count % checkpoint_interval == 0:
                    checkpoint_num += 1
                    save_checkpoint(all_company_data, failed_articles, checkpoint_num)
                    print(f"Progress: {completed_count}/{len(df_links)} companies processed")
                
                # Add a longer delay every few completions to be extra cautious
                if completed_count % 3 == 0:  # Every 3 completions
                    time.sleep(random.uniform(2.0, 4.0))
                        
            except Exception as exc:
                url = future_to_url[future]
                print(f"{url} generated an exception: {exc}")
                failed_articles.append(url)

    # Final checkpoint
    if all_company_data:
        checkpoint_num += 1
        save_checkpoint(all_company_data, failed_articles, checkpoint_num)
        print(f"Final checkpoint saved: {len(all_company_data)} total records")

    df_details = pd.DataFrame(all_company_data)
    return df_details, failed_articles

def run_failed_urls(failed_urls: list, headers: str) -> pd.DataFrame:
    """Attempts to reprocess failed company URLs from a previous scrape."""
    if not failed_urls:
        print("No failed URLs to retry.")
        return pd.DataFrame()

    print(f"Retrying {len(failed_urls)} failed URLs...")
    all_retry_data = []
    for url in tqdm(failed_urls, desc="Retrying Failed URLs"):
        try:
            details_dict = extract_company_detail(url, headers)
            dataframe_row = process_to_string(details_dict)
            all_retry_data.append(dataframe_row)
        except Exception as error:
            print(f"Giving up on url {url}. Reason: {error}")

    df_details = pd.DataFrame(all_retry_data)
    return df_details


def convert_to_csv(
    df_first_pass: pd.DataFrame, df_failed: pd.DataFrame, output_name: str
) -> pd.DataFrame:
    """Combines first-pass and retry DataFrames and writes them to a CSV file."""
    if df_failed.empty:
        print("No retry data to combine.")
        df_first_pass.to_csv(f"datasets/{output_name}.csv", index=False)
        return df_first_pass

    combined_df = pd.concat([df_first_pass, df_failed], ignore_index=True)
    combined_df.to_csv(f"datasets/{output_name}.csv", index=False)
    print(f"Successfully saved combined data to datasets/{output_name}.csv")
    return combined_df


# ----------------------------------------------------------------------------------
# REVISION 2: UPDATED MAIN EXECUTION BLOCK
# ----------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. Initialize session to get cookies and headers
    headers = True #initSession()

    if headers:
        # 2. Fetch all company links directly from the website in-memory
        # This replaces reading from `modi dataPerusahaan name link.csv`
        company_links_list = get_all_company_links(headers)
        
        # company_links_list = [{'name': '3G TRUST', 'link': 'https://minerbaone.esdm.go.id/publik/badan-usaha/detail/611426735552075729'}]
        # company_links_list = [{'name': '3G TRUST', 'link': 'https://minerbaone.esdm.go.id/publik/badan-usaha/detail/611426464528734883', 'modi_company_id': '611426464528734883'}]

        if company_links_list:
            df_links = pd.DataFrame(company_links_list)
            print(df_links)

            # 3. Scrape details for all companies with checkpointing
            # For 8000 records, save checkpoint every 100 companies
            df_first_pass, failed_urls = run_extract_company_details_v2(
                df_links, 
                checkpoint_interval=100,  # Save every 100 companies
                resume_from_checkpoint=True  # Resume from last checkpoint if available
            )

            print(f"Scraping completed!")
            print(f"Total records: {len(df_first_pass)}")
            print(f"Failed URLs: {len(failed_urls)}")

            # 4. Save final results
            if len(df_first_pass) > 0:
                final_filename = f"datasets/modi_company_all_data_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df_first_pass.to_csv(final_filename, index=False)
                print(f"✓ Final results saved to: {final_filename}")
            
            # 5. Save final failed URLs for manual review
            if failed_urls:
                failed_filename = f"datasets/final_failed_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(failed_filename, 'w') as f:
                    json.dump(failed_urls, f, indent=2)
                print(f"✓ Failed URLs saved to: {failed_filename}")

            print("\nScraping process completed.")
            print(f"Total companies processed: {len(df_first_pass)}")
        else:
            print("Could not retrieve any company links. Exiting.")
    else:
        print("Could not start scraper due to session initialization failure.")
