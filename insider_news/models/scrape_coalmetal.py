from selenium                           import webdriver
from selenium.webdriver.chrome.service  import Service
from webdriver_manager.chrome           import ChromeDriverManager
from bs4                                import BeautifulSoup
from urllib.parse                       import urljoin
from sumy.summarizers.lex_rank          import LexRankSummarizer
from sumy.parsers.plaintext             import PlaintextParser
from sumy.nlp.tokenizers                import Tokenizer
from selenium                           import webdriver
from selenium.webdriver.common.by       import By
from selenium.webdriver.support.ui      import WebDriverWait
from selenium.webdriver.support         import expected_conditions as EC
from selenium.common.exceptions         import TimeoutException

from scrapper.esdm_minerba                      import COMMODITY_MAP
from insider_news.scoring_system.scoring_engine import get_scoring_news

import pandas as pd
import logging
import time
import re 
import nltk 
import dateparser

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

# --- Configuration ---
BASE_URL = "https://coalmetal.asia"
START_URL = f"{BASE_URL}/search/indonesia" 


def get_driver(headless: bool = True) -> webdriver.Chrome:
    """ 
    Initializes a Selenium WebDriver with specified options.

    Args:
        headless (bool): If True, runs the browser in headless mode.
    
    Returns:
        webdriver.Chrome: An instance of the Chrome WebDriver.
    """
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def bypass_first_visit(url: str, timeout: float = 20.0) -> str:
    """
    Initializes a Selenium WebDriver, handles pop-ups, waits for content,
    and returns the page source.

    Args:
        url (str): The URL to load.
        timeout (float): The maximum number of seconds to wait for content.

    Returns:
        str or None: The HTML content of the page if successful, otherwise None.
    """
    driver = get_driver()
    
    # Default return value
    html_content = None  

    try:
        driver.get(url)
        
        # Handle Notification Pop-up
        try:
            LOGGER.info("Looking for notification pop-up...")
            # Using wait for the optional pop-up
            thanks_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, "//p[text()='Thanks']"))
            )
            LOGGER.info("Pop-up found. Clicking 'Thanks' to dismiss.")
            thanks_button.click()
            time.sleep(timeout)
        except TimeoutException:
            # means the pop-up didn't appear.
            LOGGER.warning("No pop-up found, continuing...")

        # Wait for the main article container to load
        LOGGER.info(f"Waiting for article container (max {timeout} seconds)...")
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CLASS_NAME, "grid-cols-2"))
        )
        LOGGER.info("Article container found.")
        
        # Get the page source
        html_content = driver.page_source
        
    except TimeoutException:
        # If the main content doesn't load in time
        LOGGER.warning(f"TimeoutException while waiting for container on {url}")
        
    except Exception as error:
        # Catch any other unexpected errors
        LOGGER.error(f"An unexpected error occurred for URL {url}: {error}")

    finally:
        # Close the browser to prevent memory leaks
        driver.quit()
        
    return html_content


def bypass_article_content(url: str, wait: float = 6.0) -> str:
    """ 
    Waits for the article content to load using Selenium and returns the HTML content.

    Args:
        url (str): The URL of the article to scrape.
        wait (float): The number of seconds to wait for the content to load.
    
    Returns:
        str: The HTML content of the article page.
    """
    driver = get_driver()

    # Navigate to the article URL
    driver.get(url)
    time.sleep(wait)
    html_content = driver.page_source
    driver.quit()
    return html_content


def get_article_links() -> list[str]:
    """
    Scrapes the main page for article links, filtering out unwanted categories.

    Returns:
        list[str]: A list of article URLs that are not in the excluded categories.
    """
    LOGGER.info("Starting browser to load the main page...")

    EXCLUDED_CATEGORIES = ['Lifestyle Etc.', 'Sustainability & Environment']
    
    # Get the page source after it's fully loaded
    html_content = bypass_first_visit(START_URL)
    LOGGER.info("Browser closed. Parsing links...")

    soup = BeautifulSoup(html_content, 'html.parser')
   
    links = []

    # Find the main container holding all the article cards
    card_container = soup.find('div', class_='grid-cols-2') 
    if not card_container:
        LOGGER.info("Error: Could not find the main article card container.")
        return []
    
    # Find all article cards within the container
    article_cards = card_container.find_all('div', class_='bg-white')
    LOGGER.info(f"Found {len(article_cards)} article cards on the page. Filtering...")
    
    # Loop through each article card to extract the category and link
    for card in article_cards:
        category_tag = card.find('p', class_='font-light')
        category = category_tag.get_text(strip=True) if category_tag else "Uncategorized"

        # The core filtering logic
        if category not in EXCLUDED_CATEGORIES:
            link_tag = card.find('a')
            if link_tag and link_tag.has_attr('href'):
                full_link = urljoin(BASE_URL, link_tag['href'])
                # Avoid duplicates
                if full_link not in links: 
                    links.append(full_link)
        else:
            LOGGER.info(f"Skipping article from excluded category: '{category}'")
    
    LOGGER.info(f"Found {len(links)} articles to scrape after filtering.")
    return links


def extract_commodities(title: str, body: str) -> list[str]:
    """
    Extract all commodity types from title and body text

    Args:
        title (str): The title of the article.
        body (str): The body text of the article.

    Returns:
        list[str]: A list of commodities found in the text.
    """
    # Combine title and body for searching
    text_to_search = f"{title} {body}".lower()
    found_commodities = []
    
    # Check for each commodity type
    for commodity_key, commodity_value in COMMODITY_MAP.items(): 
        # Check for exact commodity match (case insensitive)
        if re.search(rf'\b{re.escape(commodity_key.lower())}\b', text_to_search):
            found_commodities.append(commodity_value)
    
    # Return list of commodities found, or empty list if none found
    return found_commodities


def run_extract_commodities(title: str, body: str, full_body: str = None) -> list[str]:
    """  
    Runs the commodity extraction process on the article title, body text, and full article content.
    
    Args:
        title (str): The title of the article.
        body (str): The body text of the article.
        full_body (str): The full body text of the article.
    
    Returns:
        list[str]: A list of commodities found in the article.
    """ 
    text = f"{title} {body}".lower()
    result = any(key.lower() in text or value.lower() in text for key, value in COMMODITY_MAP.items())
        
    # Always check title + summary first
    quick_matches = extract_commodities(title, body)
    
    # If no matches but seems commodity-related, check full body
    if not quick_matches and full_body and result:
        return extract_commodities(title, full_body)
    
    return quick_matches


def get_summarize_article(text: str, sentences_count: int = 2) -> str:
    """  
    Summarizes the given text using LexRank summarization.

    Args:
        text (str): The text to summarize.
        sentences_count (int): The number of sentences to include in the summary.
    
    Returns:
        str: The summarized text.
    """
    # Create a parser for the text
    parser = PlaintextParser.from_string(text, Tokenizer('english'))        
    summarizer = LexRankSummarizer()
    # Summarize the text using LexRank
    summary = summarizer(parser.document, sentences_count)
    return ' '.join([str(sentence) for sentence in summary])


def get_article_contents(article_links: list[str]) -> list[dict]:
    """ 
    Scrapes the content of each article from the provided links.

    Args:
        article_links (list[str]): A list of article URLs to scrape.

    Returns:
        list[dict]: A list of dictionaries containing the article data.
    """
    all_articles_data = []

    for idx, article_url in enumerate(article_links):
        try:
            LOGGER.info(f"Loading article {idx+1}/{len(article_links)}: {article_url}")
        
            html_content = bypass_article_content(article_url)
            html_parsed = BeautifulSoup(html_content, 'html.parser')

            # Extract title
            title_tag = html_parsed.find('p', class_='lg:text-4xl')
            title = title_tag.get_text(strip=True) if title_tag else "Title not found"
        
            # Extract category and date
            meta_p = html_parsed.find('p', class_='lg:text-xs')
            category = meta_p.find('span').get_text(strip=True) if meta_p and meta_p.find('span') else ""
            
            # Then, get the full text from the parent p tag
            full_text = meta_p.get_text(strip=True) if meta_p else ""
            # Calculate the date by removing the original category text
            date = full_text.replace(category, "").strip()
            if '|' in date: 
                date_split = date.split('|')
                date = date_split[-1].strip()

            # Parse the date using dateparser
            parsed_date = dateparser.parse(date)
            cleaned_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S') if parsed_date else date
        
            # Find and extract the main content
            content_container = html_parsed.find('div', class_='lg:content')
            article_text = "Content not found"
            
            # Find the article contents and join it
            if content_container:
                paragraphs = content_container.find_all('p')
                # Join text from paragraphs
                article_text = "\n\n".join([paragraph.get_text(strip=True) for paragraph in paragraphs])

            # Get summarize from article content
            summarize_article = get_summarize_article(article_text)
            
            # Get commodity terms on article
            commodities = run_extract_commodities(title, summarize_article, article_text)
            
            # Get scoring system
            scoring_result = get_scoring_news(title, summarize_article, cleaned_date)
            scoring_result = scoring_result.get('news_score')

            # Output
            all_articles_data.append({
                "title": title,
                "body": summarize_article,
                "source": article_url,
                "timestamp": cleaned_date,
                "commodities": commodities, 
                "score": scoring_result
            })

        except Exception as error:
            LOGGER.error(f"Failed to process article {article_url}. Reason: {error}")
            continue

    return all_articles_data


def run_coalmetal_scraping(limit_article: int) -> pd.DataFrame:
    """  
    Runs the scraping process for CoalMetal articles and returns a DataFrame.

    Args:
        limit_article (int): The maximum number of articles to scrape.
    
    Returns:
        pd.DataFrame: A DataFrame containing the scraped article data.
    """
    all_links = get_article_links()
    scraped = get_article_contents(all_links[:limit_article])
    df = pd.DataFrame(scraped)
    return df 


if __name__ == '__main__':
    df = run_coalmetal_scraping(limit_article=10)
    # df.to_csv('coalmetal_news_test.csv', index=False)
    
