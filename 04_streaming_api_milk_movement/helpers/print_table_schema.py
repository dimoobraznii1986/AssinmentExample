"""Module for printing and inspecting DuckDB table schemas."""

import logging
from typing import List, Tuple

import duckdb

from helpers.constants import DUCKDB_FILE


def print_table_schema(table_name: str) -> List[Tuple[str, str]]:
    """Print and return the schema of the specified table."""
    with duckdb.connect(DUCKDB_FILE) as conn:
        try:
            # Get table schema
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
            logging.info("=== Table Schema ===")
            for column in schema:
                logging.info(f"Column: {column[0]}, Type: {column[1]}")
            logging.info("==================")
            return schema

        except Exception as e:
            logging.error(f"Error getting table schema: {e}")
            return []
