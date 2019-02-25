#!/usr/bin/env python
"""
Tests for the snakemake_tasks module
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import os
from shutil import rmtree

from unittest.mock import patch
from tests.helper_functions import FakeClient, FakeFetcher
from framework.tasks.snakemake_tasks import (
    check_for_runs,
    find_valid_runs,
    clone_snakemake,
    create_input_json,
)

RECORD_RESPONSE = [
    {
        "_id": {
            "sample_ids": ["A"],
            "assay": "assay_1",
            "trial": "trial_1",
            "eperimental_stategy": "foo",
            "trial_name": "ar",
        },
        "records": [
            {
                "file_name": "file_1",
                "gs_uri": "gs://lloyd-test-pipeline/foo/bar",
                "mapping": "INPUT_1",
                "_id": "bc23",
            }
        ],
    }
]

RECORD_RESPONSE_BAD = [
    {
        "_id": {
            "sample_ids": ["A"],
            "assay": "assay_2",
            "trial": "trial_1",
            "eperimental_stategy": "foo",
            "trial_name": "ar",
        },
        "records": [
            {
                "file_name": "file_1",
                "gs_uri": "gs://lloyd-test-pipeline/foo/bar",
                "mapping": "INPUT_1",
                "_id": "bc23",
            }
        ],
    }
]


def test_create_input_json():
    """
    Test create_input_json.
    """
    try:
        os.mkdir("tests/testdir")
    except FileExistsError:
        pass
    with open("tests/testdir/inputs.json", "a"):
        os.utime("tests/testdir/inputs.json", None)
    with patch(
        "json.load",
        return_value={
            "run_id": "tests/testdir",
            "meta": {"CIMAC_SAMPLE_ID": "2"},
            "sample_files": {},
            "reference_files": {"key": "value"},
        },
    ):
        with patch(
            "framework.tasks.snakemake_tasks.run_subprocess_with_logs", return_value=""
        ):
            records = [
                {
                    "mapping": "INPUT_1",
                    "gs_uri": "gs://lloyd-test-pipeline/foo",
                    "file_name": "foo.fa",
                    "_id": "1234"
                }
            ]
            create_input_json(records, "tests/testdir", "A")
            if not os.path.isfile("tests/testdir/inputs.json"):
                raise AssertionError("Inputs file not created")
            os.remove("tests/testdir/inputs.json")
            if os.path.isfile("tests/testdir/inputs.json"):
                raise AssertionError("Cleanup failed")


def test_check_for_runs():
    """
    Test check_for_runs
    """
    response = {
        "_items": [
            {
                "non_static_inputs": ["INPUT_1"],
                "assay_name": "assay 1",
                "workflow_location": "https://github.com/CIMAC-CIDC/proto",
                "_id": "123",
            }
        ]
    }
    with patch(
        "framework.tasks.snakemake_tasks.EVE.get", return_value=FakeFetcher(response)
    ):
        record_response, assay_dict = check_for_runs("token")
        if not "123" in assay_dict:
            raise AssertionError("Assay dictionary malformed")
        assay = assay_dict["123"]
        if (
            not "non_static_inputs" in assay
            or not "assay_name" in assay
            or not "workflow_location" in assay
        ):
            raise AssertionError


def test_clone_snakemake():
    """
    Test clone_snakemake
    """
    clone_snakemake("https://github.com/CIMAC-CIDC/cidc-pipeline-prototype", "fake_dir")
    if not os.path.isdir("fake_dir"):
        raise AssertionError("clone failed")
    rmtree("fake_dir")
    if os.path.isdir("fake_dir"):
        raise AssertionError("cleanup failed")


def test_find_valid_runs():
    """
    Test find_valid_runs
    """
    assay_response = {
        "assay_1": {
            "non_static_inputs": ["INPUT_1"],
            "assay_name": "foo",
            "workflow_location": "https://github.com/foo/bar",
        }
    }
    if not len(find_valid_runs(RECORD_RESPONSE, assay_response)) == 1:
        raise AssertionError
    if find_valid_runs(RECORD_RESPONSE_BAD, assay_response):
        raise AssertionError
