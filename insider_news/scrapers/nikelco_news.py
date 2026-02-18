from datetime import datetime

from insider_news.scrapers.base import Scraper

import argparse
import time 
import logging 


LOGGER = logging.getLogger(__name__)


class NikelCoIdScraper(Scraper):
    def extract_news(self, url):
        soup = self.fetch_news(url)
        article_containers = soup.select("div.td_module_10")
    
        for article in article_containers:
            title_tag = article.select_one("h3.entry-title a")
            date_tag = article.select_one("time.entry-date")
        
            if title_tag and date_tag:
                source = title_tag.get('href')
                
                # Get date and standardize 
                date = date_tag.get('datetime')
                final_date = self.standardize_date(date)

                if not final_date:
                    LOGGER.info('[NIKEL NEWS] Failed parse date for url: {source} Skipping')
                    continue

                article_data = {
                    'source': source,
                    'timestamp': final_date,
                }

                self.articles.append(article_data)
        
        LOGGER.info(f'total scraped source of nikelnews: {len(self.articles)}')
        return self.articles
    
    def standardize_date(self, date: str) -> str:
        try:
            date_dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
            final_date = date_dt.strftime("%Y-%m-%d %H:%M:%S")
            return final_date
        
        except ValueError as error:
            LOGGER.error(f"Error parse the date: {error}")
            return None 
    
    def extract_news_pages(self, num_pages):
        for page in range(1, num_pages+1):
            self.extract_news(self.get_page(page))
            time.sleep(3)
        return self.articles
   
    def get_page(self, page_num):
        return f"https://nikel.co.id/category/tambang/page/{page_num}/"
    

def main():
  scraper = NikelCoIdScraper()

  parser = argparse.ArgumentParser(description="Script for scraping data from nikel.co.id category investasi")
  parser.add_argument("page_number", type=int, default=1)
  parser.add_argument("filename", type=str, default="news_daily")
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
  python -m insider_news.models.scrape_nikel <page_number> <filename_saved> <--csv (optional)>
  '''
  main()