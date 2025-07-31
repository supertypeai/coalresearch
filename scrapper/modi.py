import requests
import pandas as pd
import json
import time

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


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


def accessPage(page: int, headers: dict) -> requests.Response:
    """Accesses a single listing page of companies."""
    url_second = f"https://modi.esdm.go.id/portal/dataPerusahaan/getdata?page={page}&sortby=id&sorttype=asc&perusahaan=&noakte="
    return requests.get(url_second, headers=headers, timeout=20)


# ----------------------------------------------------------------------------------
# REVISION 1: NEW FUNCTION TO REPLACE `downloadAllPage` and `generateCompanyData`
# ----------------------------------------------------------------------------------
def get_all_company_links(headers: dict) -> list:
    """
    Fetches all listing pages, extracts company links in memory, and returns them as a list.
    This function streamlines the process by removing the intermediate step of saving HTML files.
    """
    first_pg = 1
    last_pg = 384  # Based on the original script's hardcoded value
    all_company_links = []

    print(f"Fetching company links from {first_pg} to {last_pg} pages...")
    for i in tqdm(range(first_pg, last_pg + 1), desc="Fetching Company List Pages"):
        try:
            page_response = accessPage(i, headers)
            page_response.raise_for_status()  # Check for HTTP errors

            # Directly parse the response text and extract links
            links_on_page = extractCompanyLink(page_response.text)
            all_company_links.extend(links_on_page)

            # Small delay to be polite to the server
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"Warning: Failed to fetch page {i}. Error: {e}. Skipping...")
            continue

    print(f"Successfully extracted {len(all_company_links)} company links.")
    return all_company_links


def extractCompanyLink(html: str) -> list:
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


def process_to_string(scraped_data: dict) -> pd.DataFrame:
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
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        parsed_soup = BeautifulSoup(response.text, "html.parser")
        time.sleep(0.35)
    except requests.exceptions.RequestException as error:
        print(f"Could not fetch {url}. Error: {error}")
        return {"url": url, "error": str(error)}

    data_profile = get_profil_perusahaan(parsed_soup)
    data_alamat = get_alamat(parsed_soup)
    data_direksi = get_direksi(parsed_soup)
    data_perizinan = get_perizinan(parsed_soup)

    company_data = {**data_profile, **data_alamat, **data_direksi, **data_perizinan}
    company_data["url"] = url
    return company_data


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
    headers = initSession()

    if headers:
        # 2. Fetch all company links directly from the website in-memory
        # This replaces reading from `modi dataPerusahaan name link.csv`
        company_links_list = get_all_company_links(headers)

        if company_links_list:
            df_links = pd.DataFrame(company_links_list)

            # 3. Scrape details for all companies concurrently
            df_first_pass, failed_urls = run_extract_company_details(headers, df_links)

            # 4. Retry any failed URLs
            df_retry = run_failed_urls(failed_urls, headers)

            # 5. Combine results and save to a final CSV
            final_df = convert_to_csv(df_first_pass, df_retry, "modi_company_all_data")

            print("\nScraping process completed.")
            print(f"Total companies processed: {len(final_df)}")
        else:
            print("Could not retrieve any company links. Exiting.")
    else:
        print("Could not start scraper due to session initialization failure.")
