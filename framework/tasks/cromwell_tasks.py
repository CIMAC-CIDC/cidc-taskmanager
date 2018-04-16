#!/usr/bin/env python3
"""
Simple celery example
"""

import subprocess
import time
import json
import logging
import datetime
from os import environ as env
from typing import List
from uuid import uuid4


from google.cloud import storage
from dotenv import load_dotenv, find_dotenv
from celery import Task
from cidc_utils.requests import SmartFetch

from framework.celery.celery import APP
import constants

LOGGER = logging.getLogger('taskmanager')
LOGGER.setLevel(logging.DEBUG)

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

EVE_URL = None
CROMWELL_URL = None

if not env.get('IS_CLOUD'):
    EVE_URL = 'http://localhost:5000'
else:
    EVE_URL = (
        'http://' +
        env.get('INGESTION_API_SERVICE_HOST') + ':' + env.get('INGESTION_API_SERVICE_PORT')
    )

DOMAIN = env.get(constants.DOMAIN)
CLIENT_SECRET = env.get(constants.CLIENT_SECRET)
CLIENT_ID = env.get(constants.CLIENT_ID)
AUDIENCE = env.get(constants.AUDIENCE)
EVE_FETCHER = SmartFetch(EVE_URL)


def get_token() -> None:
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
    fetcher = SmartFetch('https://cidc-test.auth0.com/oauth/token')
    res = fetcher.post(json=payload, code=200)

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


def run_subprocess_with_logs(
        cl_args: List[str], message: str, encoding: str='utf-8', cwd: str="."
) -> None:
    """
    Runs a subprocess command and logs the output.

    Arguments:
        cl_args {[str]} -- List of string inputs to the shell command.
        message {str} -- Message that will precede output in the log.
        encoding {str} -- indicates the encoding of the shell command output.
        cwd {str} -- Current working directory.
    """
    try:
        print(message)
        subprocess.run(cl_args, cwd=cwd)
    except subprocess.CalledProcessError as error:
        error_string = 'Shell command generated error' + str(error.output)
        LOGGER.error(error_string)


def get_collabs(trial_id: str, token: str) -> dict:
    """
    Gets a list of collaborators given a trial ID

    Arguments:
        trial_id {str} -- ID of trial.
        token {str} -- Access token.

    Returns:
        dict -- Mongo response.
    """
    trial = {'_id': trial_id}
    projection = {'collaborators': 1}
    query = 'trials?where=%s&projection=%s' % (json.dumps(trial), json.dumps(projection))
    return EVE_FETCHER.get(token=token, endpoint=query)


def manage_bucket_acl(bucket_name: str, gs_path: str, collaborators: List[str]) -> None:
    """
    Manages bucket authorization for users.

    Arguments:
        bucket_name {str} -- Name of the google bucket.
        gs_path {str} -- Path to object.
        collaborators {[str]} -- List of email addresses.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    pathname = 'gs://' + bucket_name
    blob_name = gs_path.replace(pathname, '')[1:]
    blob = bucket.blob(blob_name)

    blob.acl.reload()
    for person in collaborators:
        blob.acl.user(person).grant_read()

    blob.acl.save()


@APP.task(base=AuthorizedTask)
def move_files_from_staging(upload_record, google_path):
    """Function that moves a file from staging to permanent storage

    Decorators:
        APP

    Arguments:
        upload_record {[type]} -- [description]
        google_path {str} -- Path to storage bucket.
    """
    staging_id = upload_record['_id']
    files = upload_record['files']

    for record in files:

        # Construct final data URI
        record['gs_uri'] = google_path + record['trial']['$oid'] + '/'\
         + record['assay']['$oid'] + '/' + str(uuid4()) + '/' + record['file_name']
        record['date_created'] = str(datetime.datetime.now().isoformat())
        old_uri = google_path + "staging/" + staging_id['$oid'] + '/' + record['file_name']

        # Move file to destination.
        record['trial'] = record['trial']['$oid']
        record['assay'] = record['assay']['$oid']
        gs_args = [
            'gsutil',
            'mv',
            old_uri,
            record['gs_uri']
        ]
        run_subprocess_with_logs(gs_args, "Moving Files: ")

        # Grant access to files in google storage.
        collabs = get_collabs(record['trial'], move_files_from_staging.token['access_token'])
        emails = collabs.json()['_items'][0]['collaborators']
        manage_bucket_acl('lloyd-test-pipeline', record['gs_uri'], emails)

    # when move is completed, insert data objects
    EVE_FETCHER.post(
        token=move_files_from_staging.token['access_token'], json=files, endpoint='data', code=201)


@APP.task
def hello_world(message):
    """
    Simple function to test messaging

    Decorators:
        APP

    Arguments:
        message {[type]} -- [description]
    """
    print(message)
    return message
