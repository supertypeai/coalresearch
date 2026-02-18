from datetime import datetime

from insider_news.scrapers.base import Scraper

import argparse
import time 
import logging


LOGGER = logging.getLogger(__name__)


class RuangEnergiScraper(Scraper):
    def extract_news(self, url):
        soup = self.fetch_news(url)
        article_containers = soup.select("article.elementor-post")

        for article in article_containers:
            title_tag = article.select_one("h3.elementor-post__title a")
            date_tag = article.select_one("span.elementor-post-date")

            if title_tag and date_tag:
                source = title_tag.get('href')
                
                # Get date and standardize 
                date = date_tag.get_text(strip=True)
                final_date = self.standardize_date(date)

                if not final_date:
                    LOGGER.info('[RUANGENERGI NEWS] Failed parse date for url: {source} Skipping')
                    continue

                self.articles.append({
                    'source': source,
                    'timestamp': final_date,
                })

        LOGGER.info(f'total scraped source of ruang energi: {len(self.articles)}')
        return self.articles
    
    def standardize_date(self, date: str) -> str: 
        try: 
            date_dt = datetime.strptime(date, "%d %B %Y")
            final_date = date_dt.strftime("%Y-%m-%d %H:%M:%S")
            return final_date
        
        except ValueError as error:
            LOGGER.error(f"[RUANGENERGI NEWS] Error parse the date: {error}")
            return None 

    def extract_news_pages(self, num_pages):
        base_url = [
            "https://www.ruangenergi.com/category/berita/energi-terbarukan/",
            "https://www.ruangenergi.com/category/berita/minerba/"
        ]

        for url in base_url:
            for page in range(1, num_pages + 1):
                if page == 1: 
                    target_url = url 
                else: 
                    target_url = f"{url}page/{page}/"

                self.extract_news(target_url)
                time.sleep(3)

        unique_articles = {article['source']: article for article in self.articles}
        self.articles = list(unique_articles.values())

        return self.articles
        

def main():
  scraper = RuangEnergiScraper()

  parser = argparse.ArgumentParser(description="Script for scraping data from ruangenergi category investasi")
  parser.add_argument("page_number", type=int, default=1)
  parser.add_argument("filename", type=str, default="abafarticles")
  parser.add_argument("--csv", action='store_true', help="Flag to indicate write to csv file")

  args = parser.parse_args()

  num_page = args.page_number

  scraper.extract_news_pages(num_page)
    
  scraper.write_json(scraper.articles, args.filename)

  if args.csv:
     scraper.write_csv(scraper.articles, args.filename)


if __name__ == "__main__":
    '''
    How to run:
    python -m models.scrape_nikel <page_number> <filename_saved> <--csv (optional)>
    '''
    main()
