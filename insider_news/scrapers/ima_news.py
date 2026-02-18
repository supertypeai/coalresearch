from datetime import datetime

from insider_news.scrapers.base import Scraper

import argparse
import time 
import logging 


LOGGER = logging.getLogger(__name__) 


class IMANewsScraper(Scraper):
    def extract_news(self, url: str, payload: dict):
        soup = self.fetch_news_with_post(url, payload)
        article_containers = soup.select("div.ue-grid-item")

        for article in article_containers:
            # Get source
            source = article.get('data-link')
            
            # Get date and standardize
            time_tag = article.select_one("time")
            timestamp = time_tag.get_text(strip=True) if time_tag else "Timestamp not found"
            final_date = self.standardize_date(timestamp)

            if not final_date:
                LOGGER.info('Failed parse date for url: {source} Skipping')
                continue

            self.articles.append({
                'source': source,
                'timestamp': final_date,
            })

        LOGGER.info(f'total scraped source of ima_news: {len(self.articles)}')
        return self.articles

    def standardize_date(self, date: str) -> str:
        try: 
            date_dt = datetime.strptime(date, "%m/%d/%Y") 
            final_date = date_dt.strftime("%Y-%m-%d %H:%M:%S")
            return final_date
        except ValueError as error:
            print(f"[IMA NEWS] Error parse the date: {error}")
            return None 

    def extract_news_pages(self, num_pages: int):
        ima_url = "https://ima-api.org/artikel/"
        ima_payload_mining = {
            'ucfrontajaxaction': 'getfiltersdata',
            'layoutid': '378',
            'elid': 'c67e2d3',
            'ucterms': 'category:mining',
            'addelids': 'fc62cdc'
        }
        ima_payload_investment = {
            'ucfrontajaxaction': 'getfiltersdata',
            'layoutid': '378',
            'elid': 'c67e2d3', 
            'addelids': 'fc62cdc',
            'ucs': 'investment'

        }
        ima_payload_list = [ima_payload_mining, ima_payload_investment]

        for ima_payload_post in ima_payload_list:
            for page in range(1, num_pages +1):
                payload = ima_payload_post.copy()
                payload['ucpage'] = page

                self.extract_news(ima_url, payload)
                time.sleep(5)

        return self.articles
    

def main():
    scraper = IMANewsScraper()

    parser = argparse.ArgumentParser(description="Script for scraping data from imanews")
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
    python -m models.scrape_ima <page_number> <filename_saved> <--csv (optional)>
    '''
    main()
    

