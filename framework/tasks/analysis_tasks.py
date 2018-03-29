#!/usr/bin/env python3
"""
Simple celery example
"""

import json
import logging
import re
import datetime
import os
from os import environ as env
from typing import List, Tuple
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

import requests
# from framework.utilities.rabbit_handler import RabbitMQHandler
from framework.utilities.eve_methods import request_eve_endpoint
from framework.celery.celery import APP
from framework.tasks.cromwell_tasks import run_subprocess_with_logs
from framework.tasks.cromwell_tasks import AuthorizedTask

import constants

LOGGER = logging.getLogger('taskmanager')
LOGGER.setLevel(logging.DEBUG)
# RABBIT = RabbitMQHandler('amqp://rabbitmq')
# LOGGER.addHandler(RABBIT)

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

DOMAIN = env.get(constants.DOMAIN)
CLIENT_SECRET = env.get(constants.CLIENT_SECRET)
CLIENT_ID = env.get(constants.CLIENT_ID)
AUDIENCE = env.get(constants.AUDIENCE)


def fetch_eve_or_fail(
        token: str, endpoint: str, data: dict, code: int, method: str='POST'
) -> dict:
    """
    Method for fetching results from eve with a fail safe

    Arguments:
        token {string} -- access token
        endpoint {string} -- endpoint data is being inserted into
        data {dict} -- payload data to be uploaded
        code {int} -- expected status code of the response

    Keyword Arguments:
        method {str} -- HTTP method (default: {'POST'})

    Returns:
        dict -- json formatted response from eve
    """
    response = request_eve_endpoint(token, data, endpoint, method)
    if not response.status_code == code:
        error_string = "There was a problem with your request: "
        if response.json:
            error_string += json.dumps(response.json())
        else:
            error_string += response.reason
        LOGGER.error(error_string)
        raise RuntimeError(error_string)
    return response.json()


def fetch_pipelines_bucket() -> None:
    """
    Copies the cidc-pipelines bucket to local.
    """
    # gs://lloyd-test-pipeline/cidc-pipelines/wdl
    copy_wdl_args = [
        "gsutil",
        "cp",
        "-r",
        "gs://lloyd-test-pipeline/cidc-pipelines/",
        "."
    ]
    run_subprocess_with_logs(copy_wdl_args, "Copied pipelines folder")


def dict_list_to_dict(list_of_dicts: List[dict]) -> dict:
    """
    Helper method to turn a list of dicts into one dict

    Arguments:
        list_of_dicts {[dict]} -- list of dictionaries

    Returns:
        dict -- New dictionary made of the other dictionaries.
    """
    new_dictionary = {}
    for dictionary in list_of_dicts:
        for key, value in dictionary.items():
            new_dictionary.setdefault(key, value)
    return new_dictionary


def create_analysis_entry(
        record: dict, metadata_path: str, status: bool, _etag: str, token: str
) -> dict:
    """
    Updates the analysis entry with the information from the completed run.

    Arguments:
        record {dict} -- Contextual information about the run.
        metadata_path {str} -- Path to cromwell metadata file.
        status {bool} -- Whether the run completed or aborted.
        _etag {str} -- Etag value for patching.
        token {str} -- Access token.

    Returns:
        dict -- response data from the API.
    """

    stat = "Completed" if status else "Aborted"

    payload_object = {
        'files_generated': [],
        'status': {
            'progress': stat,
            'message': ''
        }
    }

    filegen = payload_object['files_generated']
    data_to_upload = []

    with open(metadata_path, "r") as metadata:
        # Parse file string to JSON
        string_meta = metadata.read()
        payload_object['metadata_blob'] = string_meta
        outputs = json.loads(string_meta)['outputs']
        pattern = re.compile(r'gs://')

        for key, value in outputs.items():
            if re.search(pattern, str(value)):
                filegen.append({
                    'file_name': key,
                    'gs_uri': value
                })
                data_to_upload.append({
                    'file_name': key,
                    'gs_uri': value,
                    'sample_id': record['sample_id'],
                    'trial': record['trial_id'],
                    'assay': record['assay_id'],
                    'analysis_id': record['analysis_id'],
                    'date_created': str(datetime.datetime.now().isoformat())
                })
    # Insert the analysis object

    patch_res = requests.patch(
        "http://ingestion-api:5000/analysis/" + record['analysis_id'],
        json=payload_object,
        headers={
            "If-Match": _etag,
            "Authorization": token
        }
    )

    if not patch_res.status_code == 200 and not patch_res.status_code == 201:
        print("Error communicating with eve: " + patch_res.reason)
        if patch_res.json():
            print(patch_res.json())

    # Insert data
    return fetch_eve_or_fail(token, 'data', data_to_upload, 201)


def create_file_structure():
    """
    Creates the necessary directories for cromwell to run.
    """
    # Make run directory
    if not os.path.exists("/cromwell_run"):
        mkdir_args = [
            "mkdir",
            "cromwell_run"
        ]
        run_subprocess_with_logs(mkdir_args, "Making run directory: ")

    # Copy google cloud config
    if not os.path.exists("google_cloud.conf"):
        LOGGER.debug('Beginning copy of cloud config file')
        fetch_cloud_config_args = [
            "gsutil",
            "cp",
            "gs://lloyd-test-pipeline/config/google_cloud.conf",
            "."
        ]
        run_subprocess_with_logs(fetch_cloud_config_args, 'Google config copied: ')

    if not os.path.exists("cidc-pipelines"):
        LOGGER.debug('About to fetch pipelines bucket')
        fetch_pipelines_bucket()
        LOGGER.debug('Pipeline bucket fetched')


def check_for_runs() -> Tuple[requests.Response, requests.Response]:
    """
    Queries data collection and compares with assay requirements.
    If any runs are ready, sends out appropriate data.

    Returns:
        requests.Response -- Response containing data.
    """
    assay_projection = {
        'non_static_inputs': 1,
        'static_inputs': 1,
        'wdl_location': 1
    }

    assay_query_string = "assays?projection=%s" % (json.dumps(assay_projection))

    # Contains a list of all the running assays and their inputs
    assay_response = fetch_eve_or_fail(
        analysis_pipeline.token['access_token'], assay_query_string, None, 200, 'GET'
        )['_items']

    # Creates a list of all sought mappings.
    sought_mappings = [
        item for sublist in [x['non_static_inputs'] for x in assay_response] for item in sublist
    ]

    # Query the data and organize it into groupings
    # The data are grouped into unique groups via trial/assay/sample_id
    aggregate_query = {'$inputs': sought_mappings}
    query_string = "data/query?aggregate=%s" % (json.dumps(aggregate_query))
    record_response = request_eve_endpoint(
        analysis_pipeline.token['access_token'], None, query_string, 'GET'
    )

    if not record_response.status_code == 200:
        print(record_response.reason)
        return

    return record_response, assay_response


@APP.task(base=AuthorizedTask)
def analysis_pipeline():
    """
    Start of a universal "check ready" scraper.
    """

    record_response, assay_response = check_for_runs()
    groups = record_response.json()['_items']

    # Create file directory.
    create_file_structure()

    # Loop over each assay, and check for sample_id groups that
    # fulfill the input conditions.
    for assay in assay_response:
        non_static_inputs = assay['non_static_inputs']
        wdl_path = assay['wdl_location']
        for sample_assay in groups:
            if sample_assay['_id']['assay'] == assay['_id'] \
                    and len(sample_assay['records']) == len(non_static_inputs):

                assay_id = assay['_id']
                trial_id = sample_assay['_id']['trial']
                sample_id = sample_assay['_id']['sample_id']

                # Figure out why trial id is an array
                payload1 = {
                    'trial': str(trial_id[0]),
                    'assay': str(assay_id),
                    'samples': [str(sample_id)]
                }

                res_json = fetch_eve_or_fail('testing_token', 'analysis', payload1, 201)
                analysis_id = res_json['_id']

                input_dictionary = {}
                # Map inputs to make inputs.json file
                for entry in assay['static_inputs']:
                    input_dictionary[entry['key_name']] = entry['key_value']

                for record in sample_assay['records']:
                    input_dictionary[record['mapping']] = record['gs_uri']

                with open("cromwell_run/" + assay["_id"] + "_inputs.json", "w") as input_file:
                    input_file.write(json.dumps(input_dictionary))

                metadata_path = '../cromwell_run/' + assay['_id'] + '_metadata'

                # Construct cromwell arguments
                cromwell_args = [
                    'java',
                    '-Dconfig.file=../google_cloud.conf',
                    '-jar',
                    '../cromwell-30.2.jar',
                    'run',
                    wdl_path,
                    '-i',
                    '../cromwell_run/' + assay['_id'] + '_inputs.json',
                    "-m",
                    metadata_path
                ]
                LOGGER.debug('Starting cromwell run')

                # Launch run
                run_subprocess_with_logs(
                    cromwell_args, 'Cromwell run succeeded', 'utf-8', "cidc-pipelines"
                )

                LOGGER.debug("Cromwell run finished")

                # Gather metadata
                run_status = True if Path(
                    metadata_path
                ).is_file else False

                payload = {'_id': analysis_id}
                an_res = fetch_eve_or_fail(
                    analysis_pipeline.token['access_token'], 'analysis', payload, 200, 'GET'
                )
                _etag = an_res['_items'][0]['_etag']

                record = {
                    'assay_id': assay_id,
                    'trial_id': trial_id,
                    'sample_id': sample_id,
                    'analysis_id': analysis_id
                }

                create_analysis_entry(
                    record,
                    metadata_path,
                    run_status,
                    _etag,
                    analysis_pipeline.token['access_token']
                )
