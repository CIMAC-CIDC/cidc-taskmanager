#!/usr/bin/env python
"""
Simple celery example
"""

from __future__ import absolute_import, unicode_literals, print_function
import subprocess
import json
from celery_app import APP


@APP.task
def hello_world(message):
    """Simple function to test messaging
    Decorators:
        APP

    Arguments:
        message {[type]} -- [description]
    """
    print(message)
    return message


@APP.task
def run_cromwell(data):
    """Once files have been uploaded, starts a Cromwell job to process them,
    then alerts once finished.

    Decorators:
        APP - Indicates that this is a Celery worker

    Arguments:
        data {[dict]} - List of dictionary objects with file names and URIs
    """
    try:
        print(data)
        jsonized_data = json.loads(data)
        input_json = {
            "run_bwamem.bwamem.index_tar_gz": "gs://lloyd-test-pipeline/reference/hg38.canonical.bwa.tar.gz",
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
        print("json done")
        subprocess.check_output(
            [
                "gsutil",
                "cp",
                "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/bwa.wdl",
                "."
            ]
        )
        print("wdl copied")
        subprocess.check_output(
            [
                "gsutil",
                "cp",
                "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/utilities/*",
                "."
            ]
        )
        print("utilities downloaded")
        subprocess.check_output(
            [
                "gsutil",
                "cp",
                "gs://lloyd-test-pipeline/cidc-pipelines-master/wdl/configs/google_cloud.conf",
                "."
            ],
            stderr=subprocess.STDOUT
        )
        print("google config copied")
        with open("inputs.json", "w") as input_file:
            input_file.write(input_string)
        print("inputs created")
        cromwell_args = [
            'java',
            '-Dconfig.file=google_cloud.conf',
            '-jar',
            '../Cromwell/cromwell-30.2.jar',
            'run',
            "bwa.wdl",
            "--inputs",
            "inputs.json"
        ]
        subprocess.check_output(
            cromwell_args,
            stderr=subprocess.STDOUT
        )
        print("cromwell done")
        return "Succeeded!"
    except subprocess.CalledProcessError as error:
        print("Cromwell job failed: " + str(error.output))
        return "Failed!"
