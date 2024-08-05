"""
Author: Hevo
File  : run_multiple_models.py

Purpose:
--------
This script demonstrates a simple Python program that performs the basic operation of running
multiple models by sending POST requests to a specified API endpoint.

Usage Documentation:
------
Run the script to trigger multiple models.
For detailed API documentation, visit:
https://api-docs.hevodata.com/reference/introduction

License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import requests

def trigger_model(model_id):
    """
    Triggers the execution of a given model by making a POST request to the specified API endpoint.

    Parameters:
    -----------
    model_id : int
        The ID of the model to run.

    Instructions:
    --------------
    Replace '<region>' in the URL with the appropriate region for your API.
    Replace '<REPLACE_WITH_YOUR_TOKEN>' in the headers dictionary with your actual authorization token.

    Example:
    ---------
    url = f"https://us-west-1.hevodata.com/api/public/v2.0/models/{model_id}/run-now"
    headers = {
        "Authorization": "Basic YOUR_ACTUAL_AUTH_TOKEN"
    }

    Returns:
    --------
    None
    """
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

if __name__ == "__main__":
    """
    Main execution block to trigger a list of models.

    Instructions:
    --------------
    Modify the 'model_ids' list with the IDs of the models you want to run.

    Example:
    ---------
    model_ids = [123, 456, 789]

    Returns:
    --------
    None
    """
    # List of model IDs to trigger
    model_ids = [model_id1, model_id2]  # Add your model IDs here

    # Trigger each model
    for model_id in model_ids:
        trigger_model(model_id)