#!/usr/bin/env python3
"""
Celery tasks for handling some basic interaction with uploaded files.
"""
import datetime
import subprocess
import logging
import json
from typing import List
from uuid import uuid4
from google.cloud import storage
from cidc_utils.requests import SmartFetch
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.celery.celery import APP
from framework.tasks.variables import EVE_URL

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
        logging.info({
            'message': message,
            'category': 'DEBUG'
        })
        subprocess.run(cl_args, cwd=cwd)
    except subprocess.CalledProcessError as error:
        logging.error({
            'message': 'Subprocess failed',
            'category': 'ERROR-CELERY'
        }, exc_info=True)


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
        log = "Gave read access to " + person + " for object: " + gs_path
        logging.info({
            'message': log,
            'category': 'PERMISSIONS'
        })
        blob.acl.user(person).grant_read()

    blob.acl.save()


def revoke_access(bucket_name: str, gs_path: str, emails: List[str]) -> None:
    """
    Revokes access to a given object for a list of people.

    Arguments:
        bucket_name {str} -- Name of the google bucket.
        gs_path {str} -- Path to object.
        emails {[str]} -- List of email addresses.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    pathname = 'gs://' + bucket_name
    blob_name = gs_path.replace(pathname, '')[1:]
    blob = bucket.blob(blob_name)

    blob.acl.reload()
    for person in emails:
        log = "Revoked read/write access from " + person + " for object: " + gs_path
        logging.info({
            'message': log,
            'category': 'PERMISSIONS'
        })
        blob.acl.user(person).revoke_read()
        blob.acl.user(person).revoke_write()

    blob.acl.save()


@APP.task(base=AuthorizedTask)
def move_files_from_staging(upload_record: dict, google_path: str) -> None:
    """Function that moves a file from staging to permanent storage

    Decorators:
        APP

    Arguments:
        upload_record {dict} -- Ingestion collection record listing files to be uploaded.
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
        log = ("Moved record: " +
               record['file_name'] + ' from ' + old_uri + 'to ' + record['gs_uri'])
        logging.info({
            'message': log,
            'category': 'TRACK_RECORD'
        })
        # Grant access to files in google storage.
        collabs = get_collabs(record['trial'], move_files_from_staging.token['access_token'])
        emails = collabs.json()['_items'][0]['collaborators']
        manage_bucket_acl('lloyd-test-pipeline', record['gs_uri'], emails)

    # when move is completed, insert data objects
    EVE_FETCHER.post(
        token=move_files_from_staging.token['access_token'], json=files, endpoint='data', code=201
    )
