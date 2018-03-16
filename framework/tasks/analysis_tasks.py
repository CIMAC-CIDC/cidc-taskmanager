#!/usr/bin/env python3
"""
Simple celery example
"""

import json
import logging
import re
import datetime
import requests
import os
from pathlib import Path
from framework.utilities.rabbit_handler import RabbitMQHandler
from framework.utilities.eve_methods import request_eve_endpoint
from framework.celery.celery import APP
from framework.tasks.cromwell_tasks import run_subprocess_with_logs


LOGGER = logging.getLogger('taskmanager')
LOGGER.setLevel(logging.DEBUG)
# RABBIT = RabbitMQHandler('amqp://rabbitmq')
# LOGGER.addHandler(RABBIT)


def fetch_eve_or_fail(token, endpoint, data, code, method='POST'):
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
        if (response.json):
            error_string += json.dumps(response.json())
        else:
            error_string += response.reason
        LOGGER.error(error_string)
        raise RuntimeError(error_string)
    return response.json()


def fetch_pipelines_bucket():
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


def dict_list_to_dict(list_of_dicts):
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
        assay,
        trial,
        username,
        metadata_path,
        status,
        sample_id,
        analysis_id,
        _etag):

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
        json_meta = json.loads(string_meta)
        outputs = json_meta['outputs']

        file_regex = r'gs://'
        pattern = re.compile(file_regex)

        for key, value in outputs.items():
            if re.search(pattern, str(value)):
                filegen.append({
                    'file_name': key,
                    'gs_uri': value
                })
                data_to_upload.append({
                    'file_name': key,
                    'gs_uri': value,
                    'sample_id': sample_id,
                    'trial': trial,
                    'assay': assay,
                    'analysis_id': analysis_id,
                    'date_created': str(datetime.datetime.now().isoformat())
                })
    # Insert the analysis object

    patch_res = requests.patch(
        "http://ingestion-api:5000/analysis/" + analysis_id,
        json=payload_object,
        headers={
            "If-Match": _etag,
            "Authorization": 'testing_token'
        }
    )

    if not patch_res.status_code == 200 and not patch_res.status_code == 201:
        print("Error communicating with eve: " + patch_res.reason)
        if (patch_res.json()):
            print(patch_res.json())

    # Insert data
    data_response = fetch_eve_or_fail('testing_token', 'data', data_to_upload, 201)

    return data_response


@APP.task
def run_bwa_pipeline(trial, assay, username, analysis_id, _etag, samples=None):
    query_string = "assays/%s" % (assay)
    data_response = request_eve_endpoint('testing_token', None, query_string, 'GET')

    if not data_response.status_code == 200:
        print(data_response.reason)
        LOGGER.error(data_response.reason)
        return

    data_json = data_response.json()
    wdl_path = data_json['wdl_location']
    static_inputs = data_json['static_inputs']

    # Query the data and organize it into pairs
    aggregate_query = {"$trial": trial, "$assay": assay}
    query_string = "data/query?aggregate=%s" % (json.dumps(aggregate_query))
    record_response = request_eve_endpoint('testing_token', None, query_string, 'GET')

    if not record_response.status_code == 200:
        print(record_response.reason)
        return

    record_json = record_response.json()
    groups = record_json['_items']

    # Make run directory
    mkdir_args = [
        "mkdir",
        "cromwell_run"
    ]
    run_subprocess_with_logs(mkdir_args, "Making run directory: ")

    # Copy google cloud config
    LOGGER.debug('Beginning copy of cloud config file')
    fetch_cloud_config_args = [
        "gsutil",
        "cp",
        "gs://lloyd-test-pipeline/config/google_cloud.conf",
        "."
    ]
    run_subprocess_with_logs(fetch_cloud_config_args, 'Google config copied: ')

    LOGGER.debug('About to fetch pipelines bucket')
    fetch_pipelines_bucket()
    LOGGER.debug('Pipeline bucket fetched')

    for sample_id in groups:
        if not len(sample_id['records']) == 2:
            print('Error for sample: ' + sample_id['_id'] + ' not a valid pair of files')
        else:
            # Add input variables
            new_dictionary = {}

            # Merges the list of dictionaries into one dictionary.
            for entry in static_inputs:
                new_dictionary[entry['key_name']] = entry['key_value']

            # Determine Which File is which based on ending
            pattern1 = re.compile('_1\\.')
            pattern2 = re.compile('_2\\.')

            for record in sample_id['records']:
                if pattern1.search(record['file_name']):
                    new_dictionary['run_alignment.fastq1'] = record['gs_uri']
                elif pattern2.search(record['file_name']):
                    new_dictionary['run_alignment.fastq2'] = record['gs_uri']
                else:
                    print("ERROR NAMING CONVENTIONS NOT FOLLOWED")
                    raise RuntimeError("Pair naming error" + record['file_name'])

            # Create input.json file
            input_string = json.dumps(new_dictionary)
            print(input_string)
            with open("cromwell_run/inputs.json", "w") as input_file:
                input_file.write(input_string)

            # Construct cromwell arguments
            cromwell_args = [
                'java',
                '-Dconfig.file=../google_cloud.conf',
                '-jar',
                '../cromwell-30.2.jar',
                'run',
                wdl_path,
                '-i',
                '../cromwell_run/inputs.json',
                "-m",
                "../cromwell_run/metadata"
            ]
            LOGGER.debug('Starting cromwell run')

            # Launch run
            run_subprocess_with_logs(
                cromwell_args, 'Cromwell run succeeded', 'utf-8', "cidc-pipelines"
            )

            LOGGER.debug("Cromwell run finished")

            # Gather metadata
            run_status = True if Path('./cromwell_run/metadata').is_file else False

            create_analysis_entry(
                assay,
                trial,
                str(username),
                "./cromwell_run/metadata",
                run_status,
                sample_id['_id'],
                analysis_id,
                _etag
            )


@APP.task
def analysis_pipeline():
    """
    Start of a universal "check ready" scraper.
    """

    assay_projection = {
        'non_static_inputs': 1,
        'static_inputs': 1,
        'wdl_location': 1
    }

    assay_query_string = "assays?projection=%s" % (json.dumps(assay_projection))

    # Contains a list of all the running assays and their inputs
    assay_response = fetch_eve_or_fail(
        'testing_token', assay_query_string, None, 200, 'GET'
        )['_items']

    # Creates a list of all sought mappings.
    sought_mappings = [
        item for sublist in [x['non_static_inputs'] for x in assay_response] for item in sublist
    ]

    # Query the data and organize it into groupings
    # The data are grouped into unique groups via trial/assay/sample_id
    aggregate_query = {'$inputs': sought_mappings}
    query_string = "data/query?aggregate=%s" % (json.dumps(aggregate_query))
    record_response = request_eve_endpoint('testing_token', None, query_string, 'GET')

    if not record_response.status_code == 200:
        print(record_response.reason)
        return

    record_json = record_response.json()
    print(record_json)
    groups = record_json['_items']

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

    # Loop over each assay, and check for sample_id groups that
    # fulfill the input conditions.
    for assay in assay_response:
        non_static_inputs = assay['non_static_inputs']
        wdl_path = assay['wdl_location']
        for sample_assay in groups:
            if sample_assay['_id']['assay'] == assay['_id'] \
                    and len(sample_assay['records']) == len(non_static_inputs):

                input_dictionary = {}
                # Map inputs to make inputs.json file
                for entry in assay['static_inputs']:
                    input_dictionary[entry['key_name']] = entry['key_value']

                for record in sample_assay['records']:
                    input_dictionary[record['mapping']] = record['gs_uri']

                input_as_string = json.dumps(input_dictionary)

                with open("cromwell_run/" + assay["_id"] + "_inputs.json", "w") as input_file:
                    input_file.write(input_as_string)

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
                    "../cromwell_run/" + assay["_id"] + "_metadata"
                ]
                LOGGER.debug('Starting cromwell run')

                # Launch run
                run_subprocess_with_logs(
                    cromwell_args, 'Cromwell run succeeded', 'utf-8', "cidc-pipelines"
                )

                LOGGER.debug("Cromwell run finished")

                # Gather metadata
                run_status = True if Path('./cromwell_run/metadata').is_file else False
