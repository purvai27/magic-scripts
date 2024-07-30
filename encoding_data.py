"""
Author: Hevo
File  : encoding_data.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operation to
encode data.

Usage Documentation:
------
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
    data_str = str(data)  # Convert data to string
    return base64.b64encode(data_str.encode()).decode()
def transform(event):
    properties = event.getProperties()
    if 'emp_no' in properties:
        properties['emp_no'] = mask_data(properties['emp_no'])
    event.setProperties(properties)
    return event
