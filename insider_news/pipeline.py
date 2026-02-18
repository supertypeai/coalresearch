from typing_extensions import Annotated

from insider_news.scrapers.coalmetal_news import CoalMetalScraper
from insider_news.scrapers.miningcom_news import MiningScraper
from insider_news.scrapers.ima_news import IMANewsScraper
from insider_news.scrapers.nikelco_news import NikelCoIdScraper
from insider_news.scrapers.ruang_energi_news import RuangEnergiScraper
from insider_news.scrapers.base import ScraperCollection
from insider_news.scrapers.base import SeleniumScraper
from insider_news.processor import process_articles, archive_old_news

import typer 
import logging
import sys


def setup_logging():
    """
    Configures logging for the whole application
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout)
            # logging.FileHandler("scraper.log") 
        ],
        force=True
    )


app = typer.Typer(
    help='A CLI for managing scraper News',
    no_args_is_help=True
)


@app.callback()
def main():
    """
    News Scraper CLI.
    
    This callback function treats this as a multi-command app
    """
    setup_logging()


@app.command(name="scraper_daily")
def scrape_and_insert_daily_news(
    page_number: Annotated[int, typer.Option(help="Page number to scrape")] = 1,
    filename: Annotated[str, typer.Option(help="Output filename base, saved to data/daily/filename")] = "pipeline",
    batch: Annotated[int, typer.Option(help="Batch number for processing")] = 1,
    batch_size: Annotated[int, typer.Option(help="Batch size for processing")] = 75,
    process_only: Annotated[bool, typer.Option(help="Only process, don't scrape")] = False,
):
    if not process_only:
        scraper_mining = MiningScraper()
        scraper_nikel = NikelCoIdScraper()
        scraper_ima = IMANewsScraper()
        scraper_ruangenergi = RuangEnergiScraper()

        try:
            scraper_collection = ScraperCollection()
            scraper_collection.add_scraper(scraper_mining)
            scraper_collection.add_scraper(scraper_nikel)
            scraper_collection.add_scraper(scraper_ima)
            scraper_collection.add_scraper(scraper_ruangenergi)

            # Run scraper
            article_lists = scraper_collection.run_all(page_number)

            scraper_collection.write_json(article_lists, 'daily', filename)

        finally: 
            SeleniumScraper.close_shared_driver()
  
    process_articles(filename=filename, batch=batch, batch_size=batch_size, frequency='daily')


@app.command(name="scraper_weekly")
def scrape_and_insert_weekly_news(
    limit_articles: Annotated[int, typer.Option(help="Max weekly articles to scrape")] = 10,
    filename: Annotated[str, typer.Option(help="Output filename base, saved to data/weekly/filename")] = "pipeline",
    batch: Annotated[int, typer.Option(help="Batch number for processing")] = 1,
    batch_size: Annotated[int, typer.Option(help="Batch size for processing")] = 75,
    initial_run: Annotated[bool, typer.Option(help="Use CoalMetal initial-run filtering")] = False,
    process_only: Annotated[bool, typer.Option(help="Only process, don't scrape")] = False,
):
    if not process_only:
        scraper_coalmetal = CoalMetalScraper()

        try:
            article_lists = scraper_coalmetal.extract_news(
                initial_run=initial_run,
                limit=limit_articles,
            )
            scraper_coalmetal.write_json(article_lists, "weekly", filename)

        finally:
            SeleniumScraper.close_shared_driver()

    process_articles(
        filename=filename,
        batch=batch,
        batch_size=batch_size,
        frequency="weekly",
    )


@app.command(name="archive")
def archive_news(
    days_old: Annotated[int, typer.Option(help="Archive articles older than this many days")] = 182,
    db_path: Annotated[str, typer.Option(help="Path to SQLite database")] = "db.sqlite",
    archive_path: Annotated[str, typer.Option(help="Directory for archive CSV")] = "insider_news/data/archive",
    delete_from_database: Annotated[bool, typer.Option(help="Delete rows from DB after archive")] = False,
):
    _ = archive_old_news(
        days_old=days_old,
        db_path=db_path,
        archive_path=archive_path,
        delete_from_database=delete_from_database,
    )
    

if __name__ == '__main__':
    app() 


# uv run -m insider_news.pipeline scraper_weekly --limit-articles 5