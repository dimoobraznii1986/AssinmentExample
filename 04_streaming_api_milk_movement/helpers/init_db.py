"""Database initialization and configuration module for DuckDB setup."""

import logging

import duckdb

from helpers.constants import DUCKDB_FILE, TABLE_NAME
from helpers.print_table_schema import print_table_schema


def init_db() -> None:
    """Set up DuckDB database and create required tables."""
    with duckdb.connect(DUCKDB_FILE) as conn:
        # Drop existing table if it exists
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                live BOOLEAN,
                event_type TEXT,
                user_id TEXT,
                MMUserId TEXT,
                latitude DOUBLE,
                longitude DOUBLE,
                trip_id TEXT,
                trip_external_id TEXT,
                trip_created_at TIMESTAMP,
                trip_updated_at TIMESTAMP,
                trip_started_at TIMESTAMP,
                trip_MMUserId TEXT,
                route_session_type TEXT
            )
        """
        )
    print_table_schema(TABLE_NAME)
    logging.info("Database initialized successfully.")
