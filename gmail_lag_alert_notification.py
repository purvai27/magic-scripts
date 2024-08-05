"""
Author: Hevo
File  : gmail_lag_alert_notification.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operation to
notify about Gmail lag monitoring.

Usage Documentation:
------
https://api-docs.hevodata.com/reference/introduction

License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import pytz
import json
from dateutil import parser
import requests

# Email configuration
# https://docs.google.com/document/d/14TPpmngKsquoGDXT71flLZ7dQW3_DspAxeuSr07-BoE/edit?usp=sharing
SMTP_SERVER = 'smtp.gmail.com'          # For outlook use smtp.office365.com
SMTP_PORT = 587                         # port would be same for outlook
SMTP_USER = '<your_email_id>'           # Add your email id
SMTP_PASSWORD = '<your_password>'       # Add your password

# Dictionary of primary recipients and their CC recipients
EMAIL_DICT = {
    'to@email.com': [  # CC recipients
        'cc1@email.com' , 'cc2@email.com'
    ]
}

# Base URL and headers for the API request
base_url = 'https://<region>.hevodata.com/api/public/v2.0/pipelines'
headers = {
    'accept': 'application/json',
    'authorization': 'Basic <REPLACE_WITH_YOUR_TOKEN>'
}

# Define the mapping for timezones
tzinfos = {
    'EDT': -4 * 3600,  # EDT is UTC-4
    'EST': -5 * 3600,  # EST is UTC-5
    'IST': 5.5 * 3600  # IST is UTC+5:30
    # Add other timezones if needed
}

def get_pipeline_position(pipeline_id):
    """
    Fetches the display position of a given pipeline ID from the Hevo API.

    Parameters:
    pipeline_id (int): The ID of the pipeline.

    Returns:
    str: The display position of the pipeline.

    Raises:
    ValueError: If the API response does not contain the 'data' key.
    """
    url = f'{base_url}/{pipeline_id}/position'
    response = requests.get(url, headers=headers)

    # Debugging: Print the entire response
    print(f"API Response: {response.json()}")

    response_data = response.json()

    # Check if 'data' is in the response
    if 'data' not in response_data:
        raise ValueError("API response does not contain 'data' key")

    return response_data['data']['display_position']

def clean_timestamp(timestamp_str):
    """
    Cleans the timestamp string by extracting the date and time part before the comma.

    Parameters:
    timestamp_str (str): The timestamp string to be cleaned.

    Returns:
    str: The cleaned timestamp string.
    """
    return timestamp_str.split("IST,")[0].strip()//User can change the timestamp here

def parse_timestamp(timestamp_str):
    """
    Parses a timestamp string with timezone abbreviations using dateutil.parser.

    Parameters:
    timestamp_str (str): The timestamp string to be parsed.

    Returns:
    datetime: The parsed datetime object with timezone info.
    """
    return parser.parse(timestamp_str, tzinfos=tzinfos)

def check_lag(pipeline_id):
    """
    Checks the lag of a given pipeline by comparing the current time with the pipeline's display position timestamp.

    Parameters:
    pipeline_id (int): The ID of the pipeline.

    Returns:
    str: The result string containing the lag information of the pipeline.
    """
    try:
        display_position = get_pipeline_position(pipeline_id)
        print(f"Display Position: {display_position}")  # Debugging line

        timestamp_str = clean_timestamp(display_position)
        print(f"Cleaned Timestamp: {timestamp_str}")  # Debugging line

        timestamp = parse_timestamp(timestamp_str)

        # Ensure the parsed timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = pytz.timezone('Asia/Kolkata').localize(timestamp)
        else:
            timestamp = timestamp.astimezone(pytz.timezone('Asia/Kolkata')) //User need to change timezone here

        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

        lag = current_time - timestamp

        result = f"Pipeline ID: {pipeline_id}\nTimestamp: {timestamp_str}\nCurrent Time: {current_time}\nLag: {lag} hours\n"

        if lag > timedelta(hours=12):
            result += f"Warning: Pipeline {pipeline_id} has a lag of {lag.total_seconds() / 3600:.2f} hours.\n"
        elif lag > timedelta(minutes=70):
            result += f"Warning: Pipeline {pipeline_id} has a lag of {lag.total_seconds() / 60:.2f} minutes.\n"
        else:
            result += f"Pipeline {pipeline_id} is running smoothly with a lag of {lag.total_seconds() / 60:.2f} minutes.\n"
        return result
    except Exception as e:
        return f"Error processing pipeline {pipeline_id}: {e}\n"

def main(pipeline_ids):
    """
    Main function to check the lag for a list of pipeline IDs.

    Parameters:
    pipeline_ids (list): The list of pipeline IDs to check.

    Returns:
    str: The result string containing the lag information for all pipelines.
    """
    results = []
    for pipeline_id in pipeline_ids:
        result = check_lag(pipeline_id)
        results.append(result)
    return "\n".join(results)

def send_email(to_email, cc_list, subject, body):
    """
    Sends an email notification with the provided subject and body to the specified recipients.

    Parameters:
    to_email (str): The primary recipient email address.
    cc_list (list): The list of CC recipient email addresses.
    subject (str): The subject of the email.
    body (str): The body content of the email.

    Returns:
    None
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = to_email
        msg['Cc'] = ', '.join(cc_list)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        # Combine primary recipient and CC recipients
        all_recipients = [to_email] + cc_list

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, all_recipients, msg.as_string())
        print(f"Notification sent to {to_email} with CC to {', '.join(cc_list)}")
    except Exception as e:
        print(f"Failed to send email to {to_email}: {e}")

if __name__ == "__main__":
    """
    Main execution block to run the lag check and send email notifications.
    """
    subject = "Hevo Lag-Alert Notification System Results"
    pipeline_ids = [683]  # Example pipeline IDs
    results = main(pipeline_ids)
    body = f"Please find below the results of the Hevo Lag-Alert Notification System:\n\n{results}"

    for primary, cc_list in EMAIL_DICT.items():
        send_email(primary, cc_list, subject, body)