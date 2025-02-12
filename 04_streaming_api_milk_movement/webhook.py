"""Script for sending test webhook events to a local endpoint."""

import json
import time
from typing import Dict, Iterable

import requests


def send_webhook(url: str, data: Dict[str, any]) -> None:
    """Send a webhook POST request to the specified URL."""
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("Webhook sent successfully!")
    else:
        print(f"Failed to send webhook! Status code: {response.status_code}")
        print(response.text)


def load_payloads(json_file: str) -> Iterable[Dict[str, any]]:
    """Load webhook payloads from a JSON file."""
    try:
        with open(json_file, "r") as file:
            return json.load(file)

    except FileNotFoundError:
        print(f"The specified file {json_file} does not exist.")
        return []

    except json.JSONDecodeError as e:
        print(f"Error decoding the JSON. {e}")
        return []


if __name__ == "__main__":
    # The URL for the local flask app example that accepts this webhook
    webhook_url = "http://127.0.0.1:65530/webhook-endpoint"
    # JSON file with some example payloads
    payloads_file = "./files/payloads.json"

    payloads = load_payloads(payloads_file)

    if not payloads:
        print("No events to process. Stopping the app.")
    else:
        for payload in payloads:
            send_webhook(webhook_url, payload)
            time.sleep(5)
