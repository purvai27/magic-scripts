"""
Author: Hevo
File  : run_multiple_models.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operation of running
multiple models.

Usage Documentation:
------
Run the script to run the multiple models.
https://api-docs.hevodata.com/reference/introduction


License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import requests

def trigger_model(model_id):
    # Define the API endpoint URL for the given model ID
    url = f"https://<region>.hevodata.com/api/public/v2.0/models/{model_id}/run-now"

    # Define the headers with authorization
    headers = {
        "Authorization": "Basic <REPLACE_WITH_YOUR_TOKEN>"
    }

    # Make the POST request to the API endpoint
    response = requests.post(url, headers=headers)

    # Print response details for debugging
    print(f"Model ID: {model_id}")
    print("Status Code:", response.status_code)
    print("Response Text:", response.text)

# List of model IDs to trigger
model_ids = [model_id1, model_id2]  # Add your model IDs here

# Trigger each model
for model_id in model_ids:
    trigger_model(model_id)
