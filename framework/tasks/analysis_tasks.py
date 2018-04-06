#!/usr/bin/env python3
"""
Simple celery example
"""

import datetime
import json
from json import JSONDecodeError
import logging
import re
import time
from os import environ as env, path
from typing import List, Tuple
from dotenv import load_dotenv, find_dotenv

import requests
# from framework.utilities.rabbit_handler import RabbitMQHandler
from framework.utilities.eve_methods import request_eve_endpoint, smart_fetch
from framework.celery.celery import APP
from framework.tasks.cromwell_tasks import run_subprocess_with_logs, AuthorizedTask, \
    manage_bucket_acl, get_collabs

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
            try:
                error_string += json.dumps(response.json())
            except Exception as exc:
                print(exc)
                error_string += response.reason
                raise RuntimeError(error_string)
        else:
            error_string += response.reason
        LOGGER.error(error_string)
        raise RuntimeError(error_string)
    return response.json()


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
        record: dict, run_id: str, status: bool, _etag: str, token: str, emails: List[str]
) -> dict:
    """
    Updates the analysis entry with the information from the completed run.

    Arguments:
        record {dict} -- Contextual information about the run.
        run_id {str} -- Id of the run on Cromwell Server
        status {bool} -- Whether the run completed or aborted.
        _etag {str} -- Etag value for patching.
        token {str} -- Access token.
        emails: {[str]} -- List of collaborators on the trial.

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

    # If run failed, append logs to record.
    if not status:
        url = crom_url('logs', _id=run_id)
        log_data = smart_fetch(url, method='GET', code=200)
        payload_object['status']['message'] = json.dumps(log_data)

    cromwell_url = crom_url('metadata', _id=run_id)
    metadata = smart_fetch(cromwell_url, method='GET', code=200)
    filegen = payload_object['files_generated']
    data_to_upload = []

    # Parse file string to JSON
    string_meta = json.dumps(metadata)
    payload_object['metadata_blob'] = string_meta
    pattern = re.compile(r'gs://')

    for key, value in metadata['outputs'].items():
        manage_bucket_acl('lloyd-test-pipeline', value, emails)
        if re.search(pattern, str(value)):
            filegen.append({
                'file_name': key,
                'gs_uri': value
            })
            data_to_upload.append({
                'file_name': key,
                'gs_uri': value,
                'sample_id': record['sample_id'],
                'trial': record['trial'],
                'assay': record['assay'],
                'analysis_id': record['analysis_id'],
                'date_created': str(datetime.datetime.now().isoformat())
            })

    # Insert the analysis object
    patch_res = requests.patch(
        "http://ingestion-api:5000/analysis/" + record['analysis_id'],
        json=payload_object,
        headers={
            "If-Match": _etag,
            "Authorization": 'Bearer {}'.format(token)
        }
    )

    if not patch_res.status_code == 201:
        print("Error communicating with eve: " + patch_res.reason)
        if patch_res.json:
            try:
                print(patch_res.json())
            except JSONDecodeError:
                return None

    # Insert data
    return fetch_eve_or_fail(token, 'data', data_to_upload, 201)


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
        raise RuntimeError('Failed to fetch records')

    return record_response, assay_response


def create_input_json(sample_assay: dict, assay: dict) -> dict:
    """
    Constructs the input.json file to run the pipeline.

    Arguments:
        sample_assay {dict} -- Record representing a group of files with the same sampleID
        assay {dict} -- Record entry for the assay being run.

    Returns:
        dict -- Inputs.JSON
    """
    input_dictionary = {}
    # Map inputs to make inputs.json file
    for entry in assay['static_inputs']:
        input_dictionary[entry['key_name']] = entry['key_value']

    for record in sample_assay['records']:
        input_dictionary[record['mapping']] = record['gs_uri']

    return input_dictionary


def crom_url(endpoint: str=None, _id: str=None, version: str='v1') -> str:
    """
    Generates a URL formatted for the Cromwell Server API

    Keyword Arguments:
        endpoint {str} -- API Endpoint (default: {None})
        _id {str} -- Run ID (default: {None})
        version {str} -- version (default: {'v1'})

    Returns:
        str -- [description]
    """
    url = 'http://cromwell-server:8000/api/workflows/' + version
    if _id:
        url += '/' + _id
    if endpoint:
        url += '/' + endpoint
    return url


def start_cromwell_flows(assay_response, groups):
    """
    Registers a cromwell run, and sends it to the API.

    Arguments:
        assay_response {[type]} -- [description]
        groups {[type]} -- [description]
    """
    # For each registered trial/assay, check if there is data that can be run.
    response_arr = []
    for assay in assay_response:
        for sample_assay in groups:
            if sample_assay['_id']['assay'] == assay['_id'] \
                    and len(sample_assay['records']) == len(assay['non_static_inputs']):

                assay_id = assay['_id']
                trial_id = sample_assay['_id']['trial']
                sample_id = sample_assay['_id']['sample_id']

                # Figure out why trial id is an array.
                payload = {
                    'trial': str(trial_id),
                    'assay': str(assay_id),
                    'samples': [str(sample_id)]
                }

                # Create analysis entry.
                res_json = fetch_eve_or_fail(
                    analysis_pipeline.token['access_token'], 'analysis', payload, 201
                )
                payload['analysis_id'] = res_json['_id']

                # Create inputs.json file.
                files = {
                    'workflowSource': open(assay['wdl_location'], 'rb'),
                    'workflowInputs': json.dumps(create_input_json(sample_assay, assay)).encode('utf-8')
                }
                url = crom_url()
                response_arr.append({
                    'api_response': smart_fetch(url, code=201, files=files, method='POST'),
                    'analysis_etag': res_json['_etag'],
                    'record': payload
                })

    return response_arr


@APP.task(base=AuthorizedTask)
def analysis_pipeline():
    """
    Start of a universal "check ready" scraper.
    """
    # Check if any runs are ready.
    record_response, assay_response = check_for_runs()

    # If they are, send them to cromwell to start.
    active_runs = start_cromwell_flows(assay_response, record_response.json()['_items'])

    # Poll for active runs until completed.
    while active_runs:
        time.sleep(5)
        print('polling')
        LOGGER.debug('polling')
        filter_runs = []

        for run in active_runs:
            run_id = run['api_response']['id']
            record = run['record']

            # Poll status.
            response = smart_fetch(
                crom_url('status', _id=run_id), method='GET', code=200)['status']

            # Fetch collaborators to control access.
            collabs = get_collabs(record['trial'], analysis_pipeline.token['access_token'])
            emails = collabs['_items'][0]['collaborators']

            # If run has finished, make a record.
            if response not in ['Submitted', 'Running']:
                create_analysis_entry(
                    record,
                    run_id,
                    response == 'Succeeded',
                    run['_etag'],
                    analysis_pipeline.token['access_token'],
                    emails
                )
                # Add run to filter list.
                filter_runs.append(run)

        # Filter finished runs out.
        active_runs = [x for x in active_runs if x not in filter_runs]
