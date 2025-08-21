from datetime import datetime, timedelta

from insider_news.base_model.scraper                import Scraper
from .scrape_article_content                        import get_article_body
from .scrape_coalmetal                              import run_extract_commodities
from insider_news.preprocessing_llm.summary_engine  import get_summary
from insider_news.preprocessing_llm.scoring_engine  import get_scoring_news

import argparse
import time 
import random


class RuangEnergiScraper(Scraper):
    def extract_news(self, url):
        soup = self.fetch_news(url)
        article_containers = soup.select("article.elementor-post")
        print(f"Found {len(article_containers)} articles on this page ruangenergi")

        for article in article_containers:
            title_tag = article.select_one("h3.elementor-post__title a")
            date_tag = article.select_one("span.elementor-post-date")

            if title_tag and date_tag:
                # Get raw title and link
                title = title_tag.get_text(strip=True)
                source = title_tag.get('href')
                
                # Get date and standardize 
                date = date_tag.get_text(strip=True)
                final_date = self.standardize_date(date)
                if not final_date:
                    print('[RUANGENERGI NEWS] Failed parse date for url: {source} Skipping')
                    continue

                # Get article content 
                article = get_article_body(source)
                sleep_time = random.uniform(1, 3)
                time.sleep(sleep_time)

                # Skip articles with low score
                score = get_scoring_news(title, article)
                score = score.get('news_score')
                manual_score = self.manual_scoring_time(final_date)
                final_score = score + manual_score
                if final_score < 65:
                    print(f"Skipping article due to low score: {final_score}")
                    continue

                # Get summary 
                raw_summary = get_summary(article, source)
                title = raw_summary.get('title')
                body = raw_summary.get('body')

                # Extract commodities 
                commodities = run_extract_commodities(title, body, article)
                commodities = self.handling_duplicate_commodities(commodities)

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

        # Score 1: Outdated (more than 2 weeks old)
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
            date_dt = datetime.strptime(date, "%d %B %Y")
            final_date = date_dt.strftime("%Y-%m-%d %H:%M:%S")
            return final_date
        except ValueError as error:
            print(f"[RUANGENERGI NEWS] Error parse the date: {error}")
            return None 

    def extract_news_pages(self, num_pages):
        self.articles = []

        for page in range(1, num_pages + 1):
            self.extract_news(self.get_page(page))
            time.sleep(3)
        return self.articles
   
    def get_page(self, page_num):
        base_url = "https://www.ruangenergi.com/category/berita/energi-terbarukan/"
        if page_num == 1:
            return base_url
        else:
            return f"{base_url}page/{page_num}/"
        

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
    # test = 'https://www.ruangenergi.com/pln-siap-dorong-pemanfaatan-sains-dan-teknologi-untuk-akselerasi-ebt/' 
    # article = get_article_body(test)
    # print(article)