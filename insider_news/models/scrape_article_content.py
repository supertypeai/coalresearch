from bs4        import BeautifulSoup
from goose3     import Goose
from requests   import Response, Session

import requests
import os
import cloudscraper


USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "x-test": "true",
}


def get_article_body(url: str) -> str:
    """ 
    Extracts the body of an article from a given URL using Goose3.

    Args:
        url (str): The URL of the article to be extracted.
    
    Returns:
        str: The cleaned text of the article body. If extraction fails, returns an empty string
    """
    # First attempt try to get full article with goose3 proxy and soup as fallback
    try:
        proxy = os.environ.get("PROXY_KEY")
        proxy_support = {"http": proxy, "https": proxy}

        session = Session()
        session.proxies.update(proxy_support)
        session.headers.update(HEADERS)

        # g = Goose({'http_proxies': proxy_support, 'https_proxies': proxy_support})
        g = Goose({"http_session": session})
        article = g.extract(url=url)
        print(f"[SUCCESS] Article from url {url} inferenced")

        if article.cleaned_text:
            return article.cleaned_text
        else:
            # If fail, get the HTML and extract the text
            print("[REQUEST FAIL] Goose3 returned empty string, trying with soup")
            response: Response = requests.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            content = soup.find("div", class_="content")
            if content and content.get_text(strip=True):
                print(f"[SUCCESS] Article inferenced from url {url} using soup")
                return content.get_text(strip=True)

            # Fallback for ruang energi news 
            content = soup.find("div", class_="elementor-widget-theme-post-content")
            if content and content.get_text(strip=True):
                print(f"[SUCCESS] Article inferenced from url {url} using soup (.elementor-widget-theme-post-content)")
                return content.get_text(separator=" ", strip=True)
        
    except Exception as error:
        print(
            f"[PROXY FAIL] Goose3 failed with error {error} for url {url}"
        )

    # Fallback two if first attempt is completly failed
    try:
        print("[FALLBACK] Attempt 2: Trying with cloudscraper...")

        scraper = cloudscraper.create_scraper() 
        g = Goose({'browser_user_agent': USER_AGENT, 'http_session': scraper})

        article = g.extract(url=url)
        print(article)
        if article.cleaned_text:
            print(f"[SUCCESS] Extracted using cloudscraper for url {url}.")

            return article.cleaned_text
        
    except Exception as error:
        print(f"[ERROR] Cloudscraper failed: {error}")

    # Last fallback if first and second are failed
    try:
        print("[FALLBACK] Attempt 3: Trying with no PROXY...")

        g = Goose()
        article = g.extract(url=url)
        print(article)
        print(f"[SUCCESS] Article inferenced from url {url} with no PROXY")
        return article.cleaned_text
    
    except Exception as error:
        print(f"[ERROR] Goose3 with no PROXY failed with error: {error}")
    
    return ""