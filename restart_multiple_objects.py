"""
Author: Hevo
File  : restart_multiple_objects.py

Purpose:
--------
This script demonstrates a simple Python program that performs the basic operation of restarting
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

def restart_object(object_name):
    """
    Restarts a given object by making a POST request to the Hevo API.

    Parameters:
    object_name (str): The name of the object to restart.

    Returns:
    None
    """
    # Define the API endpoint URL for the given object
    url = f"https://<region>.hevodata.com/api/public/v2.0/pipelines/<id>/objects/{object_name}/restart"

    # Define the headers with authorization
    headers = {
        "Authorization": "Basic <REPLACE_WITH_YOUR_TOKEN>"
    }

    # Make the POST request to the API endpoint
    response = requests.post(url, headers=headers)

    # Print response details for debugging
    print(f"Object Name: {object_name}")
    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
    print("-" * 40)

if __name__ == "__main__":
    """
    Main execution block to restart a list of objects.
    """
    # List of objects to restart
    objects = ["employees.harman_students", "employees.Harman_Fruit"]  # Add your object names here 

    # Restart each object in a loop
    for obj in objects:
        restart_object(obj)