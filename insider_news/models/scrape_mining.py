from datetime import datetime, timedelta

from insider_news.base_model                        import Scraper
from insider_news.preprocessing_llm.scoring_engine  import get_scoring_news
from .scrape_article_content                        import get_article_body

import dateparser
import re
import argparse



COMMODITY_TYPE = {
    "Bauxite",
    "Nickel",
    "Tin",
    "Gold",
    "Copper",
    "Coal",
    "Limestone",
    "Clay",
    "Sand, Stone, Gravel",
    "Granite",
    "Non-Metallic Mineral"
}


class MiningScraper(Scraper):
    def extract_commodities(self, title, body):
        """Extract all commodity types from title and body text"""
        # Combine title and body for searching
        text_to_search = f"{title} {body}".lower()
        found_commodities = []
        
        # Check for each commodity type
        for commodity in COMMODITY_TYPE:
            # Check for exact commodity match (case insensitive)
            if re.search(rf'\b{re.escape(commodity.lower())}\b', text_to_search):
                found_commodities.append(commodity)
        
        # Return list of commodities found, or empty list if none found
        return found_commodities

    def extract_news(self, url: str):
        soup = self.fetch_news(url)
        # Scrape articles with class 'post'
        article_containers = soup.find_all("article", class_="post")
        print(f"Found {len(article_containers)} articles on this page mining.com")

        for item in article_containers:
            # Title and source (URL)
            h2 = item.find("h2")
            if h2 and h2.find("a"):
                title = h2.find("a").get_text(strip=True)
                source = h2.find("a").get("href", "").strip()
            else:
                title = None
                source = None
            
            if not title or not source:
                print(f"Skipping article due to missing title or source")
                continue
                
            # Extract article content
            article = get_article_body(source)

            # Body (summary)
            post_info = item.find("p", class_="post-info")
            body = post_info.get_text(strip=True) if post_info else ""
            
            # Timestamp (from post-meta)
            post_meta = item.find("div", class_="post-meta")
            if post_meta:
                meta_text = post_meta.get_text(separator="|", strip=True)
                parts = meta_text.split("|")
                if len(parts) >= 3:
                    date_str = parts[1].strip()
                    time_str = parts[2].strip()
                    timestamp_str = f"{date_str} {time_str}"
                elif len(parts) == 2:
                    date_str = parts[1].strip()
                    timestamp_str = date_str
                else:
                    timestamp_str = None
                # Use dateparser for flexible parsing
                dt = dateparser.parse(timestamp_str)
                if dt:
                    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = None
            else:
                timestamp = None
            
            # Skip articles with low score
            score = get_scoring_news(title, article)
            score = score.get('news_score')
            manual_score = self.manual_scoring_time(timestamp)
            final_score = score + manual_score
            if final_score < 65:
                print(f"Skipping article due to low score: {final_score}")
                continue

            # Extract all commodity types
            commodities = self.extract_commodities(title, body)
            commodities = self.handling_duplicate_commodities(commodities)

            self.articles.append(
                {
                    "title": title, 
                    "body": body, 
                    "source": source, 
                    "timestamp": timestamp,
                    "commodities": commodities  # Changed to plural and returns list
                }
            )
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
            # Representative score for the 6-8 range
            return 3 

        # Score 2: Somewhat recent (published within the last 2 weeks)
        elif time_difference <= timedelta(days=14):
            # Representative score for the 3-5 range
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
    
    def extract_news_pages(self, num_pages):
        for page in range(1, num_pages + 1):
            self.extract_news(self.get_page(page))
        return self.articles

    def get_page(self, page_num):
        return f"https://www.mining.com/page/{page_num}?s=indonesia#latest-section"


def main():
    scraper = MiningScraper()

    parser = argparse.ArgumentParser(description="Script for scraping data from mining")
    parser.add_argument("page_number", type=int, default=1)
    parser.add_argument("filename", type=str, default="idnarticles")
    parser.add_argument(
        "--csv", action="store_true", help="Flag to indicate write to csv file"
    )

    args = parser.parse_args()

    num_page = args.page_number

    scraper.extract_news_pages(num_page)

    scraper.write_json(scraper.articles, args.filename)

    if args.csv:
        scraper.write_csv(scraper.articles, args.filename)


if __name__ == "__main__":
    """
  How to run:
  python scrape_mining.py <page_number> <filename_saved> <--csv (optional)>
  """
    main()
