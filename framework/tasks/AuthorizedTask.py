#!/usr/bin/env python3
"""
Task subclass that allows processes to share a token
"""
import time
import requests
from celery import Task
from framework.tasks.variables import (
    CLIENT_SECRET, CLIENT_ID, AUDIENCE, MANAGEMENT_API, AUTH0_DOMAIN
)


def get_token() -> dict:
    """
    Fetches a token from the auth server.

    Returns:
        dict -- Server response.
    """
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'audience': AUDIENCE
    }
    res = requests.post("https://" + AUTH0_DOMAIN + "/oauth/token", json=payload)
    return {
        'access_token': res.json()['access_token'],
        'expires_in': res.json()['expires_in'],
        'time_fetched': time.time()
    }


def get_management_token() -> dict:
    """
    Gets a token for contacting the management endpoint of auth0.

    Returns:
        dict -- Dictionary with token and expirey information.
    """
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'audience': MANAGEMENT_API
    }
    res = requests.post("https://cidc-test.auth0.com/oauth/token", json=payload)
    return {
        'access_token': res.json()['access_token'],
        'expires_in': res.json()['expires_in'],
        'time_fetched': time.time()
    }


class AuthorizedTask(Task):
    """
    Subclass of the Task type, exists to allow sharing of access tokens
    between workers.

    Arguments:
        Task {Task} -- Celery task class
    """
    _token = None
    _api_token = None

    @property
    def token(self):
        """
        Defines the google access token for the class.

        Returns:
            dict -- Access response dictionary with key, ttl, time.
        """
        if self._token is None:
            self._token = get_token()
        elif time.time() - self._token['time_fetched'] > self._token['expires_in']:
            self._token = get_token()
        return self._token

    @property
    def api_token(self):
        """
        Defines the google access token for the class.

        Returns:
            dict -- Access response dictionary with key, ttl, time.
        """
        if self._api_token is None:
            self._api_token = get_management_token()
        elif time.time() - self._api_token['time_fetched'] > self._api_token['expires_in']:
            self._api_token = get_management_token()
        return self._api_token
