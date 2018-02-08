#!/usr/bin/env python3
"""
Simple celery example
"""

import subprocess
import json
import re
import os
import logging
from framework.utilities.rabbit_handler import RabbitMQHandler
from framework.celery.celery import APP


LOGGER = logging.getLogger('taskmanager')
LOGGER.setLevel(logging.DEBUG)
RABBIT = RabbitMQHandler('amqp://rabbitmq')
LOGGER.addHandler(RABBIT)


def inject_google_zones(valid_zones, wdl_directory):
    """
    Injects WDL files with the list of google zones specified

    Arguments:
        valid_zones {[str]} -- string list of valid zones for google compute engines to be launched
        wdl_directory {str} -- directory path to wdl files
    """
    zone_string = " ".join(valid_zones)
    # loop over wdl files
    for wdl in os.listdir(wdl_directory):
        with open(wdl_directory + "/" + wdl, 'r') as open_wdl:
            file_contents = open_wdl.read()
            # check if they are missing zone string
            if re.search(r'runtime', file_contents) and not re.search(r'zones:', file_contents):
                log_string = 'File: ' + wdl + 'does not have a zones argument, inserting...'
                LOGGER.info(log_string)
                with open(wdl_directory + "/" + wdl, 'r') as bad_wdl:
                    # if so, read them line by line into a temporary file
                    tmp_file = open(wdl_directory + "/" + 'tmp_' + wdl, 'a')
                    for line in bad_wdl:
                        # if that line is "runtime"
                        if re.search(r'runtime', line):
                            # write the zone string
                            spacing = ' ' * (2 * (line.count(' ') - 1))
                            tmp_file.write(line)
                            tmp_file.write(spacing + 'zones: ' + zone_string + '\n')
                        else:
                            tmp_file.write(line)
                    # replace the old file with the new one.
                    os.replace(wdl_directory + "/" + 'tmp_' + wdl, wdl_directory + "/" + wdl)
                    info_string = 'File: ' + wdl + 'replaced with new file'
                    LOGGER.info(info_string)


def run_subprocess_with_logs(cl_args, message, encoding='utf-8'):
    """
    Runs a subprocess command and logs the output.

    Arguments:
        cl_args {[string]} -- List of string inputs to the shell command.
        message {string} -- Message that will precede output in the log.
        encoding {string} -- indicates the encoding of the shell command output.
    """
    try:
        results = subprocess.run(cl_args, stdout=subprocess.PIPE)
        formatted_results = message + results.stdout.decode(encoding)
        LOGGER.debug(formatted_results)
    except subprocess.CalledProcessError as error:
        error_string = 'Shell command generated error' + str(error.output)
        LOGGER.error(error_string)


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


@APP.task
def run_cromwell(data):
    """
    Once files have been uploaded, starts a Cromwell job to process them,
    then alerts once finished.

    Decorators:
        APP - Indicates that this is a Celery worker

    Arguments:
        data {[dict]} - List of dictionary objects with file names and URIs
    """
    LOGGER.debug('Cromwell task started')
    jsonized_data = json.loads(data)
    input_json = {
        "run_bwamem.bwamem.index_tar_gz":
            "gs://lloyd-test-pipeline/reference/hg38.canonical.bwa.tar.gz",
        "run_bwamem.prefix": "test_hg38",
        "run_bwamem.fastq1": jsonized_data[0]['google_uri'],
        "run_bwamem.fastq2": jsonized_data[1]['google_uri'],
        "run_bwamem.num_cpu": "2",
        "run_bwamem.gcBias": "1",
        "run_bwamem.seqBias": "1",
        "run_bwamem.memory": "4",
        "run_bwamem.disk_space": "10",
        "run_bwamem.num_threads": "2",
        "run_bwamem.boot_disk_gb": "10",
        "run_bwamem.num_preempt": "1"
    }
    input_string = json.dumps(input_json)
    LOGGER.debug('Finished generating JSON inputs object')
    LOGGER.debug('Starting copy of WDL files')
    wdl_subprocess_args = [
        "gsutil",
        "cp",
        "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/bwa.wdl",
        "cromwell_run/"
    ]
    run_subprocess_with_logs(wdl_subprocess_args, 'Main WDL File copied: ')
    LOGGER.debug('Beginning copy of Utility WDLs')
    utility_wdl_subprocess_args = [
        "gsutil",
        "cp",
        "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/utilities/*",
        "cromwell_run/"
    ]
    run_subprocess_with_logs(utility_wdl_subprocess_args, 'Utility WDLs copied: ')
    LOGGER.debug('Beginning copy of cloud config file')
    fetch_cloud_config_args = [
        "gsutil",
        "cp",
        "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/configs/google_cloud.conf",
        "cromwell_run/"
    ]
    run_subprocess_with_logs(fetch_cloud_config_args, 'Google config copied: ')
    LOGGER.debug('Beginning generation of input JSON')
    with open("cromwell_run/inputs.json", "w") as input_file:
        input_file.write(input_string)
    LOGGER.debug('Input file generated')
    cromwell_args = [
        'java',
        '-Dconfig.file=cromwell_run/google_cloud.conf',
        '-jar',
        'cromwell-30.2.jar',
        'run',
        "cromwell_run/bwa.wdl",
        "-i",
        "cromwell_run/inputs.json"
    ]
    LOGGER.debug('Starting cromwell run')
    run_subprocess_with_logs(cromwell_args, 'Cromwell run succeeded')
    return "Succeeded!"
