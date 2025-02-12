"""Flask application for handling webhook events and storing them in DuckDB."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import duckdb
from flask import Flask, jsonify, request

from helpers.constants import DUCKDB_FILE, TABLE_NAME
from helpers.print_table_schema import print_table_schema

# Initialize Flask App
app = Flask(__name__)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("webhook_service.log"),
        logging.StreamHandler(),
    ],
)


def init_db() -> None:
    """Initialize the DuckDB database and create the table for storing webhook events."""
    with duckdb.connect(DUCKDB_FILE) as conn:
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
                route_session_type TEXT,
                process_timestamp TIMESTAMP,
                process_hour TIMESTAMP
            )
            """
        )
    print_table_schema(TABLE_NAME)
    logging.info("Database initialized successfully.")


@app.route("/webhook-endpoint", methods=["POST"])
def webhook_listener() -> Tuple[Dict[str, str], int]:
    """Handle incoming webhook POST requests."""
    try:
        data = request.json
        logging.info(f"Received webhook data: {json.dumps(data, indent=2)}")
        handle_webhook(data)
        return jsonify({"message": "Data received successfully!"}), 200

    except Exception as e:
        logging.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"error": "Failed to process data."}), 400


def handle_webhook(data: Dict[str, Any]) -> None:
    """Process the incoming webhook event and store it in DuckDB."""
    logging.info("Processing webhook data...")
    flattened_data = flatten_event(data)
    store_event(flattened_data)
    logging.info("Successfully stored webhook event.")


def flatten_event(event: Dict[str, Any]) -> Tuple[str, Any]:
    """Flatten the nested JSON webhook event into a structure suitable for storage."""
    # Get current UTC timestamp
    process_time = datetime.now(timezone.utc)
    # Truncate to hour (set minutes, seconds, microseconds to 0)
    process_hour = process_time.replace(minute=0, second=0, microsecond=0)

    logging.info(f"Processing time: {process_time}, Hour bucket: {process_hour}")

    # Get coordinates from the correct nested structure
    latitude_str = event.get("location", {}).get("coordinates", {}).get("latitude", "0")
    longitude_str = (
        event.get("location", {}).get("coordinates", {}).get("longitude", "0")
    )

    # Ensure values are valid floats
    latitude = float(latitude_str) if latitude_str not in [None, "", " "] else None
    longitude = float(longitude_str) if longitude_str not in [None, "", " "] else None

    # First create the flattened dictionary
    flattened = {
        "id": event.get("id"),
        "created_at": event.get("created_at"),
        "live": event.get("live") == "TRUE",
        "event_type": event.get("type"),
        "user_id": event.get("_id"),
        "MMUserId": event.get("MMUserId"),
        "latitude": latitude,
        "longitude": longitude,
        "trip_id": event.get("trip", {}).get("_id"),
        "trip_external_id": event.get("trip", {}).get("externalId"),
        "trip_created_at": event.get("trip", {}).get("createdAt"),
        "trip_updated_at": event.get("trip", {}).get("updatedAt"),
        "trip_started_at": event.get("trip", {}).get("startedAt"),
        "trip_MMUserId": event.get("trip", {}).get("MMUserId"),
        "route_session_type": event.get("trip", {})
        .get("metadata", {})
        .get("route_session_type"),
    }

    # Convert timestamps to proper format for DuckDB
    timestamp_fields = [
        "created_at",
        "trip_created_at",
        "trip_updated_at",
        "trip_started_at",
    ]
    for field in timestamp_fields:
        if flattened.get(field):
            timestamp = flattened[field].replace("Z", "").replace("T", " ")
            flattened[field] = timestamp
            logging.info(f"Converted timestamp {field}: {timestamp}")

    values = (
        flattened["id"],
        flattened["created_at"],
        flattened["live"],
        flattened["event_type"],
        flattened["user_id"],
        flattened["MMUserId"],
        flattened["latitude"],
        flattened["longitude"],
        flattened["trip_id"],
        flattened["trip_external_id"],
        flattened["trip_created_at"],
        flattened["trip_updated_at"],
        flattened["trip_started_at"],
        flattened["trip_MMUserId"],
        flattened["route_session_type"],
        process_time,
        process_hour,
    )

    return values


def store_event(values: Tuple[str, Any]) -> None:
    """Store the processed event data into DuckDB."""
    try:
        logging.info(f"Attempting to store values for {values[0]}")

        with duckdb.connect(DUCKDB_FILE) as conn:
            try:
                conn.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (
                        id, created_at, live, event_type, user_id, MMUserId,
                        latitude, longitude, trip_id, trip_external_id,
                        trip_created_at, trip_updated_at, trip_started_at,
                        trip_MMUserId, route_session_type, process_timestamp,
                        process_hour
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                logging.info("Insert successful")

            except Exception as db_error:
                logging.error(f"Database operation failed: {str(db_error)}")
                raise db_error

    except Exception as e:
        logging.error(f"Error in store_event: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    init_db()
    logging.info("Starting Flask server...")
    app.run(port=65530)
