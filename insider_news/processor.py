from pathlib import Path

from insider_news.preprocessing.article_enricher import build_enriched_article 
from insider_news.scrapers.base import SeleniumScraper

import sqlite3
import shutil 
import logging 
import time 
import json 
import os 


LOGGER = logging.getLogger(__name__)


def norm_source(value: object) -> str:
    return value.strip().lower() if isinstance(value, str) else ""


def get_connection(db_path: str = 'db.sqlite') -> sqlite3.Connection:
    """
    Create a SQLite connection to the specified database.
    
    Args:
        db_path: Path to the SQLite database file
    
    Returns:
        sqlite3.Connection: Connection object to the SQLite database
    """
    try:
        conn = sqlite3.connect(db_path)
        LOGGER.info(f"Connected to database at {db_path}")
        return conn
    
    except sqlite3.Error as e:
        LOGGER.error(f"Error connecting to database: {e}")
        raise
    

def create_news_table(conn: sqlite3.Connection):
    """
    Create mining_news table if it doesn't exist.
    
    Args:
        conn: SQLite connection object
    """
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS mining_news (
        id INTEGER PRIMARY KEY, 
        title TEXT NOT NULL,
        body TEXT,
        source TEXT,
        timestamp TEXT,
        commodities TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source)
    );
    """
    )
    conn.commit()
    

def get_next_id(conn: sqlite3.Connection) -> int:
    """
    Get the next available ID by finding the maximum existing ID.
    
    Args:
        conn: SQLite connection object
    
    Returns:
        int: Next available ID for insertion
    """
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) FROM mining_news;")
    result = cur.fetchone()[0]
    return (result + 1) if result is not None else 1


def insert_news_records(
    connection: sqlite3.Connection,
    processed_articles: list[dict],
):
    if not processed_articles:
        LOGGER.info("No processed articles to insert.")
        return 0

    next_available_id = get_next_id(connection)
    inserted_row_count = 0

    insert_sql = """
    INSERT INTO mining_news (
        id, title, body, source, timestamp, commodities
    ) VALUES (
        :id, :title, :body, :source, :timestamp, :commodities
    );
    """

    with connection:
        for processed_article in processed_articles:
            article_source = norm_source(processed_article.get("source"))
            article_title = (processed_article.get("title") or "").strip()

            if not article_source or not article_title:
                continue

            commodity_values = processed_article.get("commodity_type")
            if commodity_values is None:
                commodity_values = processed_article.get("commodities", [])
            if not isinstance(commodity_values, list):
                commodity_values = []

            try:
                connection.execute(
                    insert_sql,
                    {
                        "id": next_available_id,
                        "title": article_title,
                        "body": processed_article.get("body") or "",
                        "source": article_source,
                        "timestamp": processed_article.get("timestamp"),
                        "commodities": json.dumps(commodity_values, ensure_ascii=False),
                    },
                )
                next_available_id += 1
                inserted_row_count += 1

            except sqlite3.IntegrityError:
                # Duplicate source (UNIQUE constraint), skip
                continue
    
    return inserted_row_count


def load_articles(source_file: Path) -> list[dict]:
    """
    Load article payloads from a JSON file.
    """
    with source_file.open("r", encoding="utf-8") as file:
        loaded_articles = json.load(file)

    if not isinstance(loaded_articles, list):
        LOGGER.warning(
            "Expected list in %s, but got %s",
            source_file,
            type(loaded_articles).__name__,
        )
        return []

    return loaded_articles


def load_existing_sources(db_path: str) -> set[str]:
    """
    Read existing source URLs from SQLite.
    """
    database_connection = get_connection(db_path)
    try:
        create_news_table(database_connection)
        database_rows = database_connection.execute(
            "SELECT source FROM mining_news WHERE source IS NOT NULL"
        ).fetchall()

        return {
            normalized_source
            for database_row in database_rows
            for normalized_source in [norm_source(database_row[0])]
            if normalized_source
        }
    finally:
        database_connection.close()


def load_yesterday_sources(yesterday_file: Path) -> set[str]:
    """
    Load yesterday's sources for cross-run deduplication.
    """
    yesterday_sources = set()

    if not yesterday_file.exists():
        return yesterday_sources

    try:
        with yesterday_file.open("r", encoding="utf-8") as file:
            yesterday_payload = json.load(file)

        if not isinstance(yesterday_payload, list):
            return yesterday_sources

        for item in yesterday_payload:
            source_value = item.get("source") if isinstance(item, dict) else item
            normalized_source = norm_source(source_value)
            if normalized_source:
                yesterday_sources.add(normalized_source)

    except Exception as error:
        LOGGER.warning(
            "Failed reading yesterday file %s: %s. Continue without yesterday dedupe.",
            yesterday_file,
            error,
        )

    return yesterday_sources


def filter_articles_to_process(
    all_articles: list[dict],
    existing_database_sources: set[str],
    yesterday_sources: set[str],
) -> list[dict]:
    """
    Filter invalid and duplicate articles before enrichment.
    """
    seen_sources = set()
    final_articles_to_process = []

    for article in all_articles:
        if not isinstance(article, dict):
            LOGGER.info("Skipping payload because item is not a dict")
            continue

        article_source = article.get("source")
        normalized_source = norm_source(article_source)

        if not normalized_source:
            LOGGER.info("Skipping payload because source is empty")
            continue

        if (
            normalized_source in existing_database_sources
            or normalized_source in yesterday_sources
            or normalized_source in seen_sources
        ):
            continue

        seen_sources.add(normalized_source)
        final_articles_to_process.append(article)

    return final_articles_to_process


def get_article_to_process(
    jsonfile: str,
    frequency: str,
    batch: int,
    batch_size: int,
    db_path: str = "db.sqlite",
) -> list[dict]:
    """
    Load scraped articles from JSON, remove duplicates/already-processed sources,
    and return only the requested batch.
    """
    data_directory = Path(__file__).resolve().parent / "data" / frequency
    source_file = data_directory / f"{jsonfile}.json"
    filtered_file = data_directory / f"{jsonfile}_filtered.json"
    yesterday_file = data_directory / f"{jsonfile}_yesterday.json"

    try:
        if batch <= 0 or batch_size <= 0:
            LOGGER.error("batch and batch_size must be > 0")
            return []

        data_directory.mkdir(parents=True, exist_ok=True)

        if batch == 1:
            LOGGER.info("Batch 1: fresh filtering against local SQLite")

            all_articles = load_articles(source_file)
            yesterday_sources = load_yesterday_sources(yesterday_file)
            existing_database_sources = load_existing_sources(db_path)

            final_articles_to_process = filter_articles_to_process(
                all_articles=all_articles,
                existing_database_sources=existing_database_sources,
                yesterday_sources=yesterday_sources,
            )

            shutil.copy(source_file, yesterday_file)

            with filtered_file.open("w", encoding="utf-8") as file:
                json.dump(final_articles_to_process, file, indent=2, ensure_ascii=False)

            LOGGER.info(
                "Saved %s filtered articles to %s",
                len(final_articles_to_process),
                filtered_file,
            )
        else:
            LOGGER.info("Batch %s: loading pre-filtered file", batch)

            if not filtered_file.exists():
                LOGGER.error("Filtered article file not found: %s", filtered_file)
                return []

            final_articles_to_process = load_articles(filtered_file)
            LOGGER.info("Loaded %s pre-filtered articles", len(final_articles_to_process))

        total_articles = len(final_articles_to_process)
        max_needed_batches = (total_articles + batch_size - 1) // batch_size

        if total_articles == 0 or batch > max_needed_batches:
            LOGGER.info(
                "Batch %s not needed. Total=%s, max_batches=%s",
                batch,
                total_articles,
                max_needed_batches,
            )
            return []

        start_index = (batch - 1) * batch_size
        end_index = min(start_index + batch_size, total_articles)

        LOGGER.info(
            "Batch %s/%s: processing %s..%s",
            batch,
            max_needed_batches,
            start_index,
            end_index - 1,
        )
        return final_articles_to_process[start_index:end_index]

    except FileNotFoundError as error:
        LOGGER.error("Source JSON not found: %s", error)
        return []
    except (json.JSONDecodeError, sqlite3.Error, KeyError) as error:
        LOGGER.error("Failed during setup phase: %s", error, exc_info=True)
        return []


def process_articles(
    filename: str,
    batch: int,
    batch_size: int,
    frequency: str,  # "daily"/"weekly"
    minimum_score: int = 60
):
    successful_articles = []
    failed_articles_queue = []

    data_articles = get_article_to_process(
        jsonfile=filename,
        frequency=frequency, 
        batch=batch,
        batch_size=batch_size,
        db_path="db.sqlite",
    )

    if not data_articles:
        LOGGER.info(f"Batch {batch}: No articles to process.")
        return

    LOGGER.info(
        f"Batch {batch}: Processing {len(data_articles)} articles"
    )

    try: 
        for index, record_article in enumerate(data_articles):
            source_url = record_article.get("source")
            LOGGER.info(f"Processing: {source_url} {index}/{len(data_articles)}")

            try:
                processed_article = build_enriched_article(
                    record_article
                )

                time.sleep(2)

                if processed_article.get("score", 0) > minimum_score:
                    successful_articles.append(processed_article)
                else:
                    LOGGER.info(
                        f"Skipped due to low score: "
                        f"{processed_article.get('score')}"
                    )
            
            except Exception as error:
                LOGGER.error(f"Failed. Adding to retry queue. Reason: {error}")
                failed_articles_queue.append(record_article)

        for failed_data in failed_articles_queue:
            source_url = failed_data.get("source")
            LOGGER.info(f"Retrying for URL: {source_url}")

            try:
                processed_article = build_enriched_article(
                    failed_data
                )

                time.sleep(2)

                if not processed_article:
                    LOGGER.info("retry: generate_article returned None.") 

                if processed_article.get("score", 0) >= minimum_score:
                    successful_articles.append(processed_article)

            except Exception as error:
                LOGGER.error(
                    f"Failed on retry. Giving up on {source_url}: {error}"
                )
                continue 

    finally:
        LOGGER.info("All processing done. Closing Shared WebDriver.")
        SeleniumScraper.close_shared_driver()

    database_connection = get_connection("db.sqlite")

    try:
        create_news_table(database_connection)
        inserted_len = insert_news_records(database_connection, successful_articles)

        LOGGER.info("Inserted %s new records.", inserted_len)

    finally:
        database_connection.close()

