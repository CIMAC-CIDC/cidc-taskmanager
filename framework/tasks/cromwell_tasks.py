#!/usr/bin/env python3
"""
Celery tasks for handling some basic interaction with uploaded files.
"""
import subprocess
import json
import datetime
from typing import List
from uuid import uuid4
from google.cloud import storage
from cidc_utils.requests import SmartFetch
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.celery.celery import APP
from framework.tasks.variables import EVE_URL, LOGGER


EVE_FETCHER = SmartFetch(EVE_URL)


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
