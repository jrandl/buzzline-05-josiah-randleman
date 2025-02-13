"""
db_sqlite_josiahrandleman.py
"""

#####################################
# Import Modules
#####################################

import os
import pathlib
import sqlite3
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database.
    """
    logger.info(f"Initializing database at {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Table to store raw messages
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS streamed_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                author TEXT,
                sentiment REAL,
                timestamp TEXT
            )
            """)

            # Table to store category sentiment averages
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS category_sentiment (
                category TEXT PRIMARY KEY,
                avg_sentiment REAL
            )
            """)

            # Table to store author sentiment averages
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS author_sentiment (
                author TEXT PRIMARY KEY,
                avg_sentiment REAL
            )
            """)

            conn.commit()
        logger.info(f"SUCCESS: Database initialized at {db_path}")

    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")

#####################################
# Define Function to Insert Sentiment Data
#####################################

def insert_sentiment(db_path, category, author, sentiment, timestamp):
    """
    Insert sentiment data and update category and author insights.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Insert raw message into streamed_messages table
            cursor.execute("""
            INSERT INTO streamed_messages (category, author, sentiment, timestamp)
            VALUES (?, ?, ?, ?)
            """, (category, author, sentiment, timestamp))

            # Update category sentiment
            cursor.execute("""
            INSERT INTO category_sentiment (category, avg_sentiment)
            VALUES (?, ?)
            ON CONFLICT(category) DO UPDATE SET avg_sentiment = (
                SELECT AVG(sentiment) FROM streamed_messages WHERE category = ?
            )
            """, (category, sentiment, category))

            # Update author sentiment
            cursor.execute("""
            INSERT INTO author_sentiment (author, avg_sentiment)
            VALUES (?, ?)
            ON CONFLICT(author) DO UPDATE SET avg_sentiment = (
                SELECT AVG(sentiment) FROM streamed_messages WHERE author = ?
            )
            """, (author, sentiment, author))

            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to insert sentiment into database: {e}")

