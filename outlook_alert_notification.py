outlook_alert_notification.py
"""
Author: Hevo
File  : outlook_alert_notification.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operations for
sending alert notifications via email, specifically for monitoring Hevo pipeline lags.

Usage Documentation:
--------------------
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
from dateutil import parser
import requests

# Email configuration
SMTP_SERVER = 'smtp.gmail.com'          # For Outlook use smtp.office365.com
SMTP_PORT = 587                         # Port would be the same for Outlook
SMTP_USER = '<your_email_id>'           # Add your email ID
SMTP_PASSWORD = '<your_password>'       # Add your password

# Dictionary of primary recipients and their CC recipients
EMAIL_DICT = {
    'to@email.com': [  # CC recipients
        'cc1@email.com', 'cc2@email.com'
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
    Fetch the display position of a given pipeline.

    Parameters:
    -----------
    pipeline_id : int
        The ID of the pipeline.

    Returns:
    --------
    str
        The display position of the pipeline.

    Raises:
    -------
    ValueError:
        If the API response does not contain the 'data' key.
    """
    url = f'{base_url}/{pipeline_id}/position'
    response = requests.get(url, headers=headers)
    response_data = response.json()

    if 'data' not in response_data:
        raise ValueError("API response does not contain 'data' key")

    return response_data['data']['display_position']

def clean_timestamp(timestamp_str):
    """
    Clean the timestamp string to remove timezone and extra details.

    Parameters:
    -----------
    timestamp_str : str
        The raw timestamp string.

    Returns:
    --------
    str
        The cleaned timestamp string.
    """
    return timestamp_str.split("EDT,")[0].strip()

def parse_timestamp(timestamp_str):
    """
    Parse the cleaned timestamp string into a datetime object.

    Parameters:
    -----------
    timestamp_str : str
        The cleaned timestamp string.

    Returns:
    --------
    datetime
        The parsed datetime object.
    """
    return parser.parse(timestamp_str, tzinfos=tzinfos)

def check_lag(pipeline_id):
    """
    Check for the lag in the pipeline processing based on the current time.

    Parameters:
    -----------
    pipeline_id : int
        The ID of the pipeline.

    Returns:
    --------
    str
        A message describing the lag status of the pipeline.
    """
    try:
        display_position = get_pipeline_position(pipeline_id)
        print(f"Display Position: {display_position}")  # Debugging line

        timestamp_str = clean_timestamp(display_position)
        print(f"Cleaned Timestamp: {timestamp_str}")  # Debugging line

        timestamp = parse_timestamp(timestamp_str)

        # Ensure the parsed timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = pytz.timezone('America/New_York').localize(timestamp)
        else:
            timestamp = timestamp.astimezone(pytz.timezone('America/New_York'))

        current_time = datetime.now(pytz.timezone('America/New_York'))

        lag = current_time - timestamp

        result = f"Pipeline ID: {pipeline_id}\nTimestamp: {timestamp_str}\nCurrent Time: {current_time}\nLag: {lag}\n"

        if lag > timedelta(hours=12):
            result += f"Warning: Pipeline {pipeline_id} has a lag of {lag.total_seconds() / 3600:.2f} hours.\n"
        elif lag > timedelta(minutes=100):
            result += f"Warning: Pipeline {pipeline_id} has a lag of {lag.total_seconds() / 60:.2f} minutes.\n"
        else:
            result += f"Pipeline {pipeline_id} is running smoothly with a lag of {lag.total_seconds() / 60:.2f} minutes.\n"
        return result
    except Exception as e:
        return f"Error processing pipeline {pipeline_id}: {e}\n"

def main(pipeline_ids):
    """
    Main function to check lags for multiple pipelines and return the results.

    Parameters:
    -----------
    pipeline_ids : list of int
        A list of pipeline IDs to check.

    Returns:
    --------
    str
        A concatenated string of lag status messages for each pipeline.
    """
    results = []
    for pipeline_id in pipeline_ids:
        result = check_lag(pipeline_id)
        results.append(result)
    return "\n".join(results)

def send_email(to_email, cc_list, subject, body):
    """
    Send an email notification with the given subject and body.

    Parameters:
    -----------
    to_email : str
        The primary recipient's email address.
    cc_list : list of str
        A list of CC recipients' email addresses.
    subject : str
        The subject of the email.
    body : str
        The body of the email.

    Returns:
    --------
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
    subject = "Hevo Lag-Alert Notification System Results"
    pipeline_ids = [162, 548]  # Example pipeline IDs
    results = main(pipeline_ids)
    body = f"Please find below the results of the Hevo Lag-Alert Notification System:\n\n{results}"

    for primary, cc_list in EMAIL_DICT.items():
        send_email(primary, cc_list, subject, body)