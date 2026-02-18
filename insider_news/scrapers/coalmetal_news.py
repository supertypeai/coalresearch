from urllib.parse import urljoin
from bs4 import BeautifulSoup

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from insider_news.scrapers.base import SeleniumScraper

import time
import dateparser
import logging 
import argparse


LOGGER = logging.getLogger(__name__) 


class CoalMetalScraper(SeleniumScraper):
    BASE_URL = "https://coalmetal.asia"
    START_URL = f"{BASE_URL}/search/indonesia"
    EXCLUDED_CATEGORIES = ['Lifestyle Etc.', 'Sustainability & Environment', 'Opinion & Analysis']

    def __init__(self):
        super().__init__()

    def _handle_popup(self):
        """
        Dismisses the 'Thanks' popup if it appears.
        """
        try:
            thanks_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//p[text()='Thanks']"))
            )
            LOGGER.info("Pop-up found. Clicking 'Thanks'.")
            thanks_button.click()
            time.sleep(25)

        except TimeoutException:
            pass 

        except Exception as error:
            LOGGER.warning(f"Popup handling warning: {error}")

    def _get_links(self, initial_run: bool = True):
        LOGGER.info(f"Navigating to {self.START_URL}")
        
        # Ensure driver is ready
        if not self.driver:
            self.setup_driver()

        self.driver.get(self.START_URL)
        self._handle_popup()

        # Wait for grid
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "grid-cols-2"))
            )
        except TimeoutException:
            LOGGER.warning("Timeout waiting for grid-cols-2")
            return []

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        links = []

        card_container = soup.find('div', class_='grid-cols-2')
        if not card_container:
            return []

        article_cards = card_container.find_all('div', class_='bg-white')
        LOGGER.info(f"Found {len(article_cards)} cards.")

        for card in article_cards:
            p_tags = card.find_all('p', class_='font-light')
            if len(p_tags) < 2: continue

            date_text = p_tags[-1].get_text(strip=True)
            category_tags = p_tags[:-1]
            categories = [tag.get_text(strip=True).replace(' |', '').strip() for tag in category_tags]

            if len(categories) == 1 and categories[0] in self.EXCLUDED_CATEGORIES:
                LOGGER.info(f'Skipping article due to category filter: {categories[0]}')
                continue

            link_tag = card.find('a')
            if link_tag and link_tag.has_attr('href'):
                full_link = urljoin(self.BASE_URL, link_tag['href'])
                
                if initial_run and "2025" not in date_text:
                    continue
                    
                if full_link not in links:
                    links.append(full_link)

        return links

    def _scrape_single_article(self, url: str):
        try:
            LOGGER.info(f"Scraping: {url}")

            self.driver.get(url)
            time.sleep(2) 

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Extract Meta
            meta_p = soup.find('p', class_='lg:text-xs')
            full_text = meta_p.get_text(strip=True) if meta_p else ""
            
            # Extract Date
            category = meta_p.find('span').get_text(strip=True) if meta_p and meta_p.find('span') else ""
            date_raw = full_text.replace(category, "").strip()
            if '|' in date_raw:
                date_raw = date_raw.split('|')[-1].strip()

            parsed_date = dateparser.parse(date_raw)
            cleaned_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S') if parsed_date else date_raw

            return {
                
                "source": url,
                "timestamp": cleaned_date,
            }

        except Exception as error:
            LOGGER.error(f"Failed to scrape {url}: {error}")
            return None

    def extract_news(self, initial_run: bool = False, limit: int = 5):
        all_links = self._get_links(initial_run)
        
        # Scrape content for each link
        for _, link in enumerate(all_links[:limit]):
            data = self._scrape_single_article(link)
            
            if data:
                self.articles.append(data)
                
        return self.articles


def main():
  scraper = CoalMetalScraper()

  parser = argparse.ArgumentParser(description="Script for scraping data from antaranews")
  parser.add_argument("limit_article", type=int, default=10)
  parser.add_argument("filename", type=str, default="anataranews")
  parser.add_argument("--csv", action='store_true', help="Flag to indicate write to csv file")

  args = parser.parse_args()

  scraper.extract_news(args.limit_article)
    
  scraper.write_json(scraper.articles, args.filename)

  if args.csv:
     scraper.write_csv(scraper.articles, args.filename)


if __name__ == '__main__':
    main()