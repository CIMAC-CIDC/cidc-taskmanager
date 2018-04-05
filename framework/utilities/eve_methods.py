#!/usr/bin/env python3
"""[summary]
"""

import requests
from simplejson.errors import JSONDecodeError


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


def smart_fetch(
        url: str,
        token: str=None, files: dict=None,
        data: dict=None, method: str='POST', code: int=200, params: dict=None
) -> dict:
    """
    Method to make HTTP requests in a logical, error safe way.

    Arguments:
        url {str} -- URL being requested

    Keyword Arguments:
        token {str} -- Access token if endpoint protected. (default: {None})
        files {str} -- Files to upload if any are being uploaded. (default: {None})
        data {dict} -- Data being posted. (default: {None})
        method {str} -- The HTTP method. (default: {'POST'})
        code {int} -- Code indicating success. (default: {200})
        params {dict} -- Parameters passed to endpoint. (default: {None})

    Raises:
        KeyError -- [description]

    Returns:
        dict -- JSON formatted response.
    """
    method_dict = {
        'GET': requests.get,
        'POST': requests.post,
        'PUT': requests.put,
        'HEAD': requests.head,
        'OPTIONS': requests.options,
        'DELETE': requests.delete
    }

    if method not in method_dict:
        error_string = 'Method argument ' + method + ' not a valid operation'
        raise KeyError(error_string)

    request_func = method_dict[method]
    headers = {}

    if token:
        headers['Authorization'] = 'Bearer {}'.format(token)

    response = request_func(
        url,
        json=data,
        params=params,
        headers=headers,
        files=files
    )

    if not response.status_code == code:
        print('Request to: ' + url + ' failed.')
        print(response.reason)
        try:
            print(response.json())
        except JSONDecodeError:
            return None

    return response.json()
