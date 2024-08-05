encoding_data.py
"""
Author: Hevo
File  : encoding_data.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operations to
encode data, specifically using base64 encoding.

Usage Documentation:
--------------------
https://api-docs.hevodata.com/reference/introduction

License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import base64
from io.hevo.api import Event

"""
event: each record streaming through Hevo pipeline is an event
returns:
    - The modified event object.
    - Array of event objects if new events are generated from the incoming event.
    - None if the event is supposed to be dropped from the pipeline.
Read complete documentation at: https://docs.hevodata.com/pipelines/transformations/
"""

def mask_data(data):
    """
    Encode the given data using base64 encoding.

    Parameters:
    -----------
    data : any
        The data to encode. It will be converted to a string before encoding.

    Returns:
    --------
    str
        The base64 encoded string of the input data.
    """
    data_str = str(data)  # Convert data to string
    return base64.b64encode(data_str.encode()).decode()

def transform(event):
    """
    Transform the event by encoding specific properties.

    Parameters:
    -----------
    event : Event
        The event object containing the data to be transformed.

    Returns:
    --------
    Event
        The modified event object with encoded properties.
    """
    properties = event.getProperties()
    if 'object_name' in properties: #Object name is the name of object you want to encode.
        properties['object_name'] = mask_data(properties['object_name'])
    event.setProperties(properties)
    return event

