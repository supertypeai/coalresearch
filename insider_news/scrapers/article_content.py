from bs4 import BeautifulSoup
from goose3 import Goose
from requests import Response, Session
from insider_news.scrapers.base import SeleniumScraper

import requests
import os
import cloudscraper
import logging 
import time 


LOGGER = logging.getLogger(__name__)


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
    if 'coalmetal.asia' in url: 
        selenium_scraper = SeleniumScraper()
        selenium_scraper.driver.get(url)
        time.sleep(3) 

        soup = BeautifulSoup(selenium_scraper.driver.page_source, 'html.parser')
        content_container = soup.find('div', class_='lg:content')
        article_text = "Content not found"

        if content_container:
            paragraphs = content_container.find_all('p')
            article_text = "\n\n".join([p.get_text(strip=True) for p in paragraphs])
            return article_text 
            
    try:
        proxy = os.environ.get("PROXY_KEY")
        proxy_support = {"http": proxy, "https": proxy}

        session = Session()
        session.proxies.update(proxy_support)
        session.headers.update(HEADERS)

        # g = Goose({'http_proxies': proxy_support, 'https_proxies': proxy_support})
        g = Goose({"http_session": session})
        article = g.extract(url=url)
        LOGGER.info(f"[SUCCESS] Article from url {url} inferenced")

        if article.cleaned_text:
            return article.cleaned_text
        else:
            # If fail, get the HTML and extract the text
            LOGGER.info("[REQUEST FAIL] Goose3 returned empty string, trying with soup")
            response: Response = requests.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            content = soup.find("div", class_="content")
            if content and content.get_text(strip=True):
                LOGGER.info(f"[SUCCESS] Article inferenced from url {url} using soup")
                return content.get_text(strip=True)

            # Fallback for ruang energi news 
            content = soup.find("div", class_="elementor-widget-theme-post-content")
            if content and content.get_text(strip=True):
                LOGGER.info(f"[SUCCESS] Article inferenced from url {url} using soup (.elementor-widget-theme-post-content)")
                return content.get_text(separator=" ", strip=True)
        
    except Exception as error:
        LOGGER.error(
            f"[PROXY FAIL] Goose3 failed with error {error} for url {url}"
        )

    try:
        LOGGER.info("[FALLBACK] Attempt 2: Trying with cloudscraper...")

        scraper = cloudscraper.create_scraper() 
        g = Goose({'browser_user_agent': USER_AGENT, 'http_session': scraper})

        article = g.extract(url=url)
        
        if article.cleaned_text:
            LOGGER.info(f"[SUCCESS] Extracted using cloudscraper for url {url}.")
            return article.cleaned_text
        
    except Exception as error:
        LOGGER.error(f"[ERROR] Cloudscraper failed: {error}")

    try:
        LOGGER.info("[FALLBACK] Attempt 3: Trying with no PROXY...")

        g = Goose()
        article = g.extract(url=url)

        LOGGER.info(article)
        LOGGER.info(f"[SUCCESS] Article inferenced from url {url} with no PROXY")
        return article.cleaned_text
    
    except Exception as error:
        LOGGER.error(f"[ERROR] Goose3 with no PROXY failed with error: {error}")
    
    LOGGER.info('All approach get article body failed, return None')
    return None