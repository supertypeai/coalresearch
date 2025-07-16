import requests
import pandas as pd

from bs4 import BeautifulSoup

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

if __name__ == '__main__':
    # # Run one time
    # downloadAllPage()

    company_link_list = generateCompanyData()
    company_link_df = pd.DataFrame(company_link_list)
    company_link_df.to_csv("datasets/modi dataPerusahaan name link.csv")