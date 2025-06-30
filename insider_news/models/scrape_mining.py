import argparse
import sys
import os
from bs4 import BeautifulSoup
from datetime import datetime
import dateparser
import re

# Add the parent directory (project root) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from base_model import Scraper

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

    def extract_news(self, url):
        soup = self.fetch_news(url)
        # Scrape articles with class 'post'
        for item in soup.find_all("article", class_="post"):
            # Title and source (URL)
            h2 = item.find("h2")
            if h2 and h2.find("a"):
                title = h2.find("a").get_text(strip=True)
                source = h2.find("a").get("href", "").strip()
            else:
                title = ""
                source = ""
            
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
                    timestamp_str = ""
                # Use dateparser for flexible parsing
                dt = dateparser.parse(timestamp_str)
                if dt:
                    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = ""
            else:
                timestamp = ""
            
            # Extract all commodity types
            commodities = self.extract_commodities(title, body)
            
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

    def extract_news_pages(self, num_pages):
        for i in range(1, num_pages + 1):
            self.extract_news(self.get_page(i))
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
