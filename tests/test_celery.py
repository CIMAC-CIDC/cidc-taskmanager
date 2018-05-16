#!/usr/bin/env python3
"""
Unit tests for the celery repo
"""
from framework.tasks.analysis_tasks import create_input_json
from framework.tasks.cromwell_tasks import run_subprocess_with_logs


def test_run_subprocess():
    """
    Tests the run_subprocess
    """
    run_subprocess_with_logs(["ls"], "")
    assert True


def test_create_input_json():
    """
    Tests function create_input_json
    """
    sample_assay = {
        'records': [
            {
                'gs_uri': 'gs://sample/gs/uri',
                'mapping': ''
            }
        ]
    }
    assay = {
        'static_inputs': [
            {
                'key_name': 'key name',
                'key_value': 'key value'
            }
        ]
    }
    input_dictionary = create_input_json(sample_assay, assay)
    assert input_dictionary
