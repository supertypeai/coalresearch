from datetime       import datetime, timedelta

from insider_news.base_model.scraper                import Scraper
from insider_news.preprocessing_llm.summary_engine  import get_summary
from insider_news.preprocessing_llm.scoring_engine  import get_scoring_news
from .scrape_article_content                        import get_article_body
from .scrape_coalmetal                              import run_extract_commodities

import argparse
import time 
import random


class IMANewsScraper(Scraper):
    def extract_news(self, url: str, payload: dict):
        soup = self.fetch_news_with_post(url, payload)
        article_containers = soup.select("div.ue-grid-item")
        print(f"Found {len(article_containers)} articles on this page IMA news")

        for article in article_containers:
            # Get source
            source = article.get('data-link')
            # Get raw title
            # title_tag = article.select_one("h4.elementor-heading-title")
            # title = title_tag.get_text(strip=True) if title_tag else "Title not found"
            
            # Get date and standardize
            time_tag = article.select_one("time")
            timestamp = time_tag.get_text(strip=True) if time_tag else "Timestamp not found"
            final_date = self.standardize_date(timestamp)
            if not final_date:
                print('[IMA NEWS] Failed parse date for url: {source} Skipping')
                continue

            # Get article content  
            article_content = get_article_body(source)
            sleep_time = random.uniform(1, 3)
            time.sleep(sleep_time)
            
            # Skip articles with low score
            score = get_scoring_news(title, article_content)
            score = score.get('news_score')
            manual_score = self.manual_scoring_time(final_date)
            final_score = score + manual_score
            if final_score < 65:
                print(f"Skipping article due to low score: {final_score}")
                continue
            
            # Get summary
            raw_summary = get_summary(article_content, source)
            title = raw_summary.get('title')
            body = raw_summary.get('body')

            #Get commodities from article
            commodities = run_extract_commodities(title, body, article_content)
            commodities = self.handling_duplicate_commodities(commodities)

            if source and title and timestamp:
                self.articles.append({
                    'title': title,
                    'body': body,
                    'source': source,
                    'timestamp': final_date,
                    'commodities': commodities
                })

        return self.articles

    def manual_scoring_time(self, date: str): 
        if isinstance(date, str):
            publication_timestamp = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        current_time = datetime.now() 

        # scoring manual for timestamp 
        time_difference = current_time - publication_timestamp 

        # Score 5: Very recent (published within the last 48 hours)
        if time_difference <= timedelta(hours=48):
            return 5
    
        # Score 3: Recent (published within the last week)
        elif time_difference <= timedelta(days=7):
            return 3 

        # Score 2: Somewhat recent (published within the last 2 weeks)
        elif time_difference <= timedelta(days=14):
            return 2 

        # Score 1: Outdated 
        else:
            return 1
        
    def handling_duplicate_commodities(self, commodities: list) -> list:
        seen = set()
        update = []
        for commodity in commodities:
            if commodity not in seen:
                update.append(commodity)
                seen.add(commodity)
        return update 

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
    python -m models.scrape_ima <page_number> <filename_saved> <--csv (optional)>
    '''
    main()
    

