import requests
import pandas as pd
import json 
import time

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def initSession():
    url_initial = 'https://modi.esdm.go.id/portal/dataPerusahaan'
    headers_initial = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0',
    }

    resp_initial = requests.get(url_initial, headers=headers_initial)

    cookies = resp_initial.cookies.get_dict()
    cookie_string = '; '.join([f'{key}={value}' for key, value in cookies.items()])
    print("Cookie header string:", cookie_string)

    headers_second = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'modi.esdm.go.id',
        'Referer': 'https://modi.esdm.go.id/portal/dataPerusahaan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': headers_initial['User-Agent'],
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cookie': cookie_string,  # Inject the cookies
    }

    return headers_second


def accessPage(page: int, headers: dict) -> requests.Response:
	url_second = f'https://modi.esdm.go.id/portal/dataPerusahaan/getdata?page={page}&sortby=id&sorttype=asc&perusahaan=&noakte='
	return requests.get(url_second, headers=headers)


def downloadAllPage():
    first_pg = 1
    last_pg = 384

    headers = initSession()

    for i in range(first_pg, last_pg + 1):
        page_response = accessPage(i, headers)
        file_name = f"datasets/modi dataPerusahaan/modi dataPerusahaan page_{i}.html"
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(page_response.text)
            print("Saved", file_name)


def extractCompanyLink(html: str) -> list:
        
    soup = BeautifulSoup(html, 'html.parser')

    results = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if '/portal/detailPerusahaan/' in href:
            name = a_tag.text.strip()
            results.append({'name': name, 'link': href})

    return results


def generateCompanyData() -> list:
    company_data = []

    for i in range(1, 385):
        file_name = f"datasets/modi dataPerusahaan/modi dataPerusahaan page_{i}.html"
        
        with open(file_name, "r", encoding='utf-8') as f:
            content = f.read()
            print("Parsing", file_name)
            company_data += extractCompanyLink(content)
    
    return company_data


def parse_html_table(table_element: BeautifulSoup) -> list:
    """
    Parses an HTML table element into a list of dictionaries using table headers as keys.

    Args:
        table_element (BeautifulSoup): A <table> element with <thead> and <tbody> sections.

    Returns:
        list: List of row dictionaries mapped by header text. Returns empty list if invalid table.
    """
    if not table_element or not table_element.find('thead'):
        return []
    
    headers = [th.text.strip() for th in table_element.find('thead').find_all('th')]
    rows = []
    for tr in table_element.find('tbody').find_all('tr'):
        cells = [td.text.strip() for td in tr.find_all('td')]
        # Ensure the number of cells matches headers to avoid errors
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    return rows


def get_profil_perusahaan(soup: BeautifulSoup) -> dict:
    """
    Extracts structured company profile information from an HTML document.

    Args:
        soup (BeautifulSoup): A parsed BeautifulSoup object of the company's profile page.

    Returns:
        dict: Dictionary containing structured profile data and table contents. Includes 
              an 'error' key if the main container is not found.
    """
    # Extract profil perusahaan
    profile_container = soup.find('div', id='profile')
    if not profile_container:
        return {"error": "Profile tab container not found."}
    
    # Initialize data output
    data = {}

    # Extract the main "Profil Perusahaan" key-value data 
    profil_table = profile_container.find('table')
    profil_data = {}
    if profil_table:
        for row in profil_table.find('tbody').find_all('tr'):
            key_element = row.find('th')
            value_elements = row.find_all('td')
            if key_element and value_elements:
                # Key is in 'th', value is in the last 'td'
                key = key_element.text.strip()
                value = value_elements[-1].text.strip()
                profil_data[key] = value
    data['profil_perusahaan'] = profil_data

    # Extract data from all other sections and their tables 
    sections = profile_container.find_all('div', class_='row')
    for section in sections:
        title_tag = section.find('b')
        if not title_tag:
            continue

        # Clean up the title to use as a dictionary key
        title_text = title_tag.text.strip().split('. ')[-1].lower().replace(' ', '_').replace('/', '_')
        if title_text == "pemilik___pemegang_saham":
            title_text = "pemilik_pemegang_saham"

        # Find the table within this section
        table = section.find('table')
        if table:
            # Parse the table using our helper function
            table_data = parse_html_table(table)
            # Only add if data was found
            if table_data: 
                data[title_text] = table_data
                
    return data


def get_alamat(soup: BeautifulSoup) -> dict:
    """
    Extracts address history from the 'Alamat' section of a company's HTML profile.

    Args:
        soup (BeautifulSoup): A parsed BeautifulSoup object of the company's profile page.

    Returns:
        dict: Dictionary containing address history. Returns an 'error' key if the container is missing.
    """
    alamat_container = soup.find('div', id='alamat')
    if not alamat_container:
        return {"error": "Alamat tab container not found."}
    
    # Initialize data output
    data = {}

    # Each address history item is in a 'timeline-item' div
    timeline_items = alamat_container.find_all('div', class_='timeline-item')
    
    history = []
    for item in timeline_items:
        table = item.find('table')
        if table:
            # Parse the table into structured details
            details = parse_html_table(table)
            # Ensure the table isn't empty
            if details:  
                entry = {}
                # Extract section title (e.g., date or description)
                title_tag = item.find('h5')
                entry['title'] = title_tag.text.strip() if title_tag else 'Alamat Historis'
                entry['details'] = details
                history.append(entry)
            
    if history:
        data['alamat_history'] = history
            
    return data


def get_direksi(soup: BeautifulSoup) -> dict:
    """
    Extracts historical board of directors (direksi) information from the HTML document.

    Args:
        soup (BeautifulSoup): A parsed BeautifulSoup object of the company's profile page.

    Returns:
        dict: Dictionary containing board history. Returns an 'error' key if the container is missing.
    """
    # Locate the 'direksi' section container
    direksi_container = soup.find('div', id='direksi')
    if not direksi_container:
        return {"error": "Direksi tab container not found."}

    data = {}
    # Each board record is inside a 'timeline-item' div
    timeline_items = direksi_container.find_all('div', class_='timeline-item')
    
    history = []
    for item in timeline_items:
        table = item.find('table')
        # Only proceed if a table with data exists within the item
        if table:
            details = parse_html_table(table)
            if details:  # Ensure the table isn't empty
                entry = {}
                # Extract section title (e.g., term or year)
                title_tag = item.find('h5')
                entry['title'] = title_tag.text.strip() if title_tag else 'Direksi Historis'
                entry['details'] = details
                history.append(entry)
    
    # Add board history only if any valid entries found
    if history:
        data['direksi_history'] = history
            
    return data


def get_perizinan(soup: BeautifulSoup) -> dict: 
    """
    Extracts business license (perizinan) data from the HTML document.

    Args:
        soup (BeautifulSoup): A parsed BeautifulSoup object of the company's profile page.

    Returns:
        dict: Dictionary containing license data. Returns an 'error' key if the container is missing.
    """
    # Locate the 'perizinan' section container
    perizinan_container = soup.find('div', id='perizinan')
    if not perizinan_container:
        return {"error": "Perizinan tab container not found."}

    data = {}

    # Find the specific table for perizinan by its ID
    table = perizinan_container.find('table', id='dt_basics')
    if table:
        table_data = parse_html_table(table)
        if table_data:
            data['perizinan_data'] = table_data
    
    return data


def process_to_string(scraped_data: dict) -> pd.DataFrame:
    """
    Converts nested scraped data (lists/dicts) into JSON-formatted strings.

    Args:
        scraped_data (dict): Dictionary containing raw scraped data.

    Returns:
        pd.DataFrame: A single-row dictionary with all nested structures stringified for storage or export.
    """
    processed_row = {}
    for key, value in scraped_data.items():
        # Convert list/dict values to a readable JSON string
        if isinstance(value, (list, dict)) and value:
            processed_row[key] = json.dumps(value, ensure_ascii=False, indent=2)
        else: 
            processed_row[key] = value
    return processed_row


def extract_company_detail(url: str, headers: str) -> dict:
    """
    Extracts detailed company information from a given URL using multiple HTML parsers.

    Args:
        url (str): Target URL of the company's profile page.
        headers (str): HTTP headers to include in the request.

    Returns:
        dict: Combined dictionary containing profile, address, board, and license data.
              Includes 'url' and 'error' if the request fails.
    """
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        parsed_soup = BeautifulSoup(response.text, 'html.parser')

        # Short delay to respect server load
        time.sleep(0.35)
    except requests.exceptions.RequestException as error:
        print(f"Could not fetch {url}. Error: {error}")
        return {"url": url, "error": str(error)}
    
      # Extract structured data from various sections of the tabs
    data_profile = get_profil_perusahaan(parsed_soup)
    data_alamat = get_alamat(parsed_soup)
    data_direksi = get_direksi(parsed_soup)
    data_perizinan = get_perizinan(parsed_soup)
    
    # Merge all dictionaries
    company_data = {**data_profile, **data_alamat, **data_direksi, **data_perizinan}
    company_data['url'] = url

    return company_data


def run_extract_company_details(headers: str, df_links: pd.DataFrame) -> tuple[pd.DataFrame, list]: 
    """
    Runs concurrent extraction of company details using a pool of threads.

    Args:
        headers (str): HTTP headers to pass into each request.
        df_links (pd.DataFrame): DataFrame containing a 'link' column with target URLs.

    Returns:
        tuple[pd.DataFrame, list]: Extracted data as a DataFrame and a list of failed URLs.
    """
    all_company_data = []
    failed_articles = []

    MAX_WORKERS = 10 
            
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all scraping tasks to the executor
        future_to_url = {executor.submit(extract_company_detail, row['link'], headers): row['link'] for _, row in df_links.iterrows()}
        
        for future in tqdm(as_completed(future_to_url), total=len(df_links), desc="Scraping Companies"):
            url = future_to_url[future]
            try:
                details_dict = future.result()

                # Log failed attempts based on response content
                if "error" in details_dict:
                    print(f"Appending url {url} to failed urls")
                    failed_articles.append(url)

                # Format result for DataFrame compatibility
                dataframe_row = process_to_string(details_dict)
                all_company_data.append(dataframe_row)

            except Exception as exc:
                url = future_to_url[future]
                print(f'{url} generated an exception: {exc}')

    
    df_details = pd.DataFrame(all_company_data)
    return df_details, failed_articles


def run_failed_urls(failed_urls: list, headers: str) -> pd.DataFrame: 
    """
    Attempts to reprocess failed company URLs from a previous scrape.

    Args:
        failed_urls (list): List of URLs that failed during the first pass.
        headers (str): HTTP headers to include in retry requests.

    Returns:
        pd.DataFrame: DataFrame containing retried data. Empty if all retries failed.
    """
    if not failed_urls:
        print("No Failed urls need to retry")
        return pd.DataFrame()

    all_rety_url = []

    for url in failed_urls:
        try:
            details_dict = extract_company_detail(url, headers)
            dataframe_row = process_to_string(details_dict)
            all_rety_url.append(dataframe_row)
        except Exception as error: 
            print(f'Give up for url {url} reason: {error}')
    
    df_details = pd.DataFrame(all_rety_url)
    return df_details


def convert_to_csv(df_first_pass: pd.DataFrame, 
                   df_failed: pd.DataFrame, output_name: str) -> pd.DataFrame:
    """
    Combines first-pass and retry DataFrames (if any) and writes them to a CSV file.

    Args:
        df_first_pass (pd.DataFrame): Main scraped results from the first pass.
        df_failed (pd.DataFrame): Additional rows from failed URL retries.
        output_name (str): File name (without extension) for the final CSV output.

    Returns:
        pd.DataFrame: Combined DataFrame that was saved to CSV.
    """
    if df_failed.empty:
        print("No need to combined two dataframe")
        df_first_pass.to_csv(f"datasets/{output_name}.csv", index=False)
        return df_first_pass
    
    combined_df = pd.concat([df_first_pass, df_failed], ignore_index=True)
    combined_df.to_csv(f"datasets/{output_name}.csv", index=False)
    return combined_df


if __name__ == '__main__':
    headers = initSession()
    df = pd.read_csv('datasets/modi dataPerusahaan name link.csv')
    df_first, failed = run_extract_company_details(headers, df)
    df_retry = run_failed_urls(failed, headers)
    final_df = convert_to_csv(df_first, df_retry, "modi_company_all")

    # test_url = df['link'][0]
    # print(test_url)
    # test_url2 = "https://modi.esdm.go.id/portal/detailPerusahaan/16620?jp=1"
    # df_scrape = extract_company_detail(test_url, headers)
    # df_scrape.to_csv("test_scraped.csv", index=False)

    #  # Run one time
    # downloadAllPage()

    # company_link_list = generateCompanyData()
    # company_link_df = pd.DataFrame(company_link_list)
    # company_link_df.to_csv("datasets/modi dataPerusahaan name link.csv")