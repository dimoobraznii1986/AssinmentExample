#!/usr/bin/env bash
set -e -o pipefail

# Runs flask endpoint and then sends payloads to the endpoint
trap "kill 0" EXIT  # Kill all child processes when the script exits
python3 main.py &  # Start the flask server in the background
sleep 5  # Give Flask some time to initialize fully
python3 webhook.py & # Start the webhook script

wait  # Wait for all child processes to complete
