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
from os import environ as env
from typing import List, Tuple
from dotenv import load_dotenv, find_dotenv
from cidc_utils.requests import SmartFetch

import requests
# from framework.utilities.rabbit_handler import RabbitMQHandler
from framework.celery.celery import APP
from framework.tasks.cromwell_tasks import AuthorizedTask, manage_bucket_acl, get_collabs

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
EVE_URL = env.get(constants.EVE_URL)
CROMWELL_URL = env.get(constants.CROMWELL_URL)
EVE_FETCHER = SmartFetch(EVE_URL)
CROMWELL_FETCHER = SmartFetch(CROMWELL_URL)


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
        endpoint = run_id + '/' + 'logs'
        log_data = CROMWELL_FETCHER.get(endpoint=endpoint).json()
        payload_object['status']['message'] = json.dumps(log_data)

    endpoint = run_id + '/' + 'metadata'
    metadata = CROMWELL_FETCHER.get(endpoint=endpoint).json()
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
    return EVE_FETCHER.post(token=token, endpoint='data', json=data_to_upload, code=201)


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
    assay_response = EVE_FETCHER.get(
        token=analysis_pipeline.token['access_token'], endpoint=assay_query_string, code=200
    ).json()['_items']

    # Creates a list of all sought mappings.
    sought_mappings = [
        item for sublist in [x['non_static_inputs'] for x in assay_response] for item in sublist
    ]

    # Query the data and organize it into groupings
    # The data are grouped into unique groups via trial/assay/sample_id
    aggregate_query = {'$inputs': sought_mappings}
    query_string = "data/query?aggregate=%s" % (json.dumps(aggregate_query))
    record_response = EVE_FETCHER.get(
        token=analysis_pipeline.token['access_token'], endpoint=query_string)

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
                res_json = EVE_FETCHER.post(
                    token=analysis_pipeline.token['access_token'],
                    endpoint='analysis',
                    json=payload,
                    code=201
                ).json()
                payload['analysis_id'] = res_json['_id']

                # Create inputs.json file.
                files = {
                    'workflowSource': open(assay['wdl_location'], 'rb'),
                    'workflowInputs': json.dumps(
                        create_input_json(sample_assay, assay)
                    ).encode('utf-8')
                }
                response_arr.append({
                    'api_response': CROMWELL_FETCHER.post(code=201, files=files).json(),
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
        LOGGER.debug('polling')
        filter_runs = []

        for run in active_runs:
            print(run)
            run_id = run['api_response']['id']
            record = run['record']

            # Poll status.
            endpoint = run_id + '/' + 'status'
            response = CROMWELL_FETCHER.get(
                endpoint=endpoint, method='GET', code=200).json()['status']

            print(response)

            # If run has finished, make a record.
            if response not in ['Submitted', 'Running']:

                # Fetch collaborators to control access.
                collabs = get_collabs(record['trial'], analysis_pipeline.token['access_token'])
                emails = collabs.json()['_items'][0]['collaborators']

                create_analysis_entry(
                    record,
                    run_id,
                    response == 'Succeeded',
                    run['analysis_etag'],
                    analysis_pipeline.token['access_token'],
                    emails
                )
                # Add run to filter list.
                filter_runs.append(run)

        # Filter finished runs out.
        active_runs = [x for x in active_runs if x not in filter_runs]
