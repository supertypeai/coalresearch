import sqlite3
from insider_news.utils.config import LOGGER

def get_connection(db_path: str = 'db.sqlite') -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(db_path)
        LOGGER.info(f"Connected to database at {db_path}")
        return conn
    except sqlite3.Error as e:
        LOGGER.error(f"Error connecting to database: {e}")
        raise
    