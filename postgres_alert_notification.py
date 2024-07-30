"""
Author: Hevo
File  : postgres_alert_notification.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operation of restarting
multiple objects.

Usage Documentation:
------
https://api-docs.hevodata.com/reference/introduction


License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import requests
from datetime import datetime, timedelta
import pytz
import json
from dateutil import parser

# Slack webhook URL
slack_webhook_url = '<YOUR_SLACK_URL>'

# Base URL and headers for the API request
base_url = 'https://<region>.hevodata.com/api/public/v2.0/pipelines'
headers = {
    'accept': 'application/json',
    'authorization': 'Basic <YOUR_TOKEN>'
}

# Define the mapping for timezones
tzinfos = {
    'EDT': -4 * 3600,  # EDT is UTC-4
    'EST': -5 * 3600,  # EST is UTC-5
    # Add other timezones if needed
}

def get_pipeline_position(pipeline_id):
    url = f'{base_url}/{pipeline_id}/position'
    response = requests.get(url, headers=headers)
    response_data = response.json()
    return response_data['data']['display_position']

def parse_timestamp(timestamp_str):
    # Parse datetime with timezone abbreviation using dateutil.parser and tzinfos
    return parser.parse(timestamp_str, tzinfos=tzinfos)

def check_lag_and_notify(pipeline_id):
    try:
        display_position = get_pipeline_position(pipeline_id)
        timestamp_str = display_position.split(", Seq No")[0].strip()
        timestamp = parse_timestamp(timestamp_str)

        # Ensure the parsed timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=pytz.utc)

        edt = pytz.timezone('US/Eastern')
        current_time = datetime.now(edt)

        # Convert current_time to UTC before subtracting to avoid timezone issues
        current_time_utc = current_time.astimezone(pytz.utc)
        lag = current_time_utc - timestamp

        print(f"Pipeline ID: {pipeline_id}")
        print(f"Timestamp: {timestamp_str}")
        print(f"Current Time: {current_time}")
        print(f"Lag: {lag}")

        if lag > timedelta(hours=12):
            notify_slack(pipeline_id, lag)
    except Exception as e:
        print(f"Error processing pipeline {pipeline_id}: {e}")


def notify_slack(pipeline_id, lag):
    message = f"Pipeline {pipeline_id} has a lag of {lag.total_seconds() / 3600:.2f} hours."
    payload = {
        'text': message
    }
    response = requests.post(slack_webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise ValueError(f'Failed to send message to Slack: {response.text}')

def main(pipeline_ids):
    for pipeline_id in pipeline_ids:
        check_lag_and_notify(pipeline_id)

if __name__ == "__main__":
    # Example pipeline IDs
    pipeline_ids = [75,63]  #You can add more pipelines here
    main(pipeline_ids)
