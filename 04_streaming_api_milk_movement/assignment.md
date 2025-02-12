# Locations Takehome

## Overview

Every day, thousands of drivers using Milk Moovement (MM) run milk delivery routes across North America. Each driver has a cellular device running a third party piece of software to capture regular location updates. These location updates are then sent to MM and must be ingested and stored. Having regular and accurate location updates from every driver is crucial to various aspects of our business.

## The Task

​Design a service to handle ingestion and storage of location data provided by the third party service. Use the example code provided in this repository to build a simple example of how you would handle input data from a webhook. Build your solution in the `main.py` file's `handle_webhook` function.

As an additional exercise, include design for how you would handle this kind of task in production. This service should be built using AWS tools/services. A simple writeup will suffice for this part of the assignment. Bonus points for diagrams/illustrations.

## Key requirements

- Third party sends location updates one at a time via webhook. Example payload:

```json
{
  "event": {
    "createdAt": "2023-07-24T19:52:07.507Z",
    "type": "user.updated_trip",
    "location": {
      "type": "Point",
      "coordinates": [
        -119.3056079094478,
        36.00942850116281
      ]
    },
    "user": {
      "_id": "6477c8c614fe6c005a89df2b",
      "updatedAt": "2023-07-24T19:52:07.507Z",
      "MMUserId": "619481dd940bbe643baf776e",
      "trip": {
        "_id": "64bebf33fc633e0061e84882",
        "createdAt": "2023-07-24T18:13:07.159Z",
        "updatedAt": "2023-07-24T18:13:07.159Z",
        "externalId": "1f76eed0-992a-4d17-b532-462f089a42e8",
        "MMUserId": "619481dd940bbe643baf776e",
        "startedAt": "2023-07-24T18:13:07.159Z",
        "metadata": {
          "route_session_type": "RawMilk",
          "route_session_id": "64bebf32d38530406ca4f254"
        }
      }
    }
  }
}

```

- The location updates should be normalized before stored / shared
- Ideal location destination is stored in an optimized format in an S3 based data lake or directly in our Snowflake warehouse
- Include an example of how to send events to an event bus (Kinesis for example) when particular webhook event type is received (for example, type == user.entered_geofence or user.exited_geofence)

## Running

This task needs to run on a Linux/Unix based operating system with access to the Bash shell. Any Linux distro, MacOS or [WSL instance](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-11-with-gui-support#1-overview) should work fine.

You'll need to install **[Poetry (Python dependency manager)](https://python-poetry.org/docs/)** to run and develop the assignment. Once installed you can see if your solution is working by running.

```bash
poetry install
```
To install required dependencies and set up your virtual environments. Then run the application with:

```bash
poetry run bash run_test.sh
```

Don't forget the written portion of the assignment! The total time shouldn't take more than a couple hours to finish everything.

## Extra Credit

- Demonstrate how to aggregate stored location data to publish high level analytics per day (Number of geofences crosses per day)

## Evaluation

- Your submission will be evaluated based on the following criteria:
    - Completeness (does it satisfy the basic requirements? Are edge cases considered?)
    - Extensibility (can new features be quickly added?)
- Send your submission via email (zipped folder)
- As a final step in the interview process, we will ask you to do a quick (less than 15 minutes) walkthrough of the solution along with time for Q&A
