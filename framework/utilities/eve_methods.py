#!/usr/bin/env python3
"""[summary]
"""

import requests


def request_eve_endpoint(eve_token, payload_data, endpoint, method='POST'):
    """
    Generic method for running a request against the API with authorization

    Arguments:
        eve_token {str} -- API token
        payload_data {dict} -- The payload to be sent
        endpoint {str} -- Name of the endpoint the request should be sent to

    Returns:
        obj -- Returns request object
    """

    method_dictionary = {
        'GET': requests.get,
        'POST': requests.post,
        'PUT': requests.put,
        'HEAD': requests.head,
        'OPTIONS': requests.options,
        'DELETE': requests.delete
    }
    if method not in method_dictionary:
        error_string = 'Method argument ' + method + ' not a valid operation'
        raise KeyError(error_string)

    request_func = method_dictionary[method]
    return request_func(
        "http://ingestion-api:5000" + "/" + endpoint,
        json=payload_data,
        headers={"Authorization": 'Bearer {}'.format(eve_token)}
    )
