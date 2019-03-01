"""
Tasks related to WDL pipelines.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"


import json
import logging
import re
from typing import List, Tuple

import requests
from cidc_utils.requests import SmartFetch
from framework.tasks.variables import EVE_URL

EVE_FETCHER = SmartFetch(EVE_URL)


def create_input_json(sample_assay: dict, assay: dict) -> dict:
    """
    Constructs the input.json file to run the pipeline.

    Arguments:
        sample_assay {dict} -- Record representing a group of files with the same sampleID.
        assay {dict} -- Record entry for the assay being run.

    Returns:
        dict -- Inputs.JSON
         record_response: [{
            _id: {
                sample_id: "...",
                assay: "...",
                trial: "..."
            },
            records: [
                {
                    file_name: "...",
                    gs_uri: "...",
                    mapping: "...",
                    _id: "..."
                }
            ]
        }]
    """
    input_dictionary = {}

    # Get SampleID of run.
    sample_id = sample_assay["_id"]["sample_id"]

    run_prefix = assay["static_inputs"][0]["key_name"].split(".")[0]
    # Map inputs to make inputs.json file
    for entry in assay["static_inputs"]:

        # Set the prefix using the sample ID.
        if re.search(r".prefix$", entry["key_name"]):
            input_dictionary[entry["key_name"]] = sample_id
        else:
            input_dictionary[entry["key_name"]] = entry["key_value"]

    for record in sample_assay["records"]:
        if not re.search(run_prefix, record["mapping"]):
            input_dictionary[run_prefix + "." + record["mapping"]] = record["gs_uri"]
        else:
            input_dictionary[record["mapping"]] = record["gs_uri"]

    input_message = "Input Dictionary Created: \n" + json.dumps(input_dictionary)
    logging.info({"message": input_message, "category": "INFO-CELERY-DEBUG"})
    return input_dictionary


def set_record_processed(records: List[dict], condition: bool, token: str) -> bool:
    """
    Takes a list of records, then changes their
    processed status to match the condition.

    Arguments:
        records {List[dict]} -- List of "data" collection records.
        condition {bool} -- True if processed else false.
        token {str} -- JWT

    Returns:
        bool -- True if all records were succesfully patched, else false.
    """
    patch_status = []
    for record in records:
        patch_res = requests.patch(
            EVE_URL + "/data_edit/" + record["_id"],
            json={"processed": condition},
            headers={
                "If-Match": record["_etag"],
                "Authorization": "Bearer {}".format(
                    token
                ),
            },
        )
        patch_status.append(patch_res.status_code == 200)

    return all(patch_status)


def check_processed(records: List[dict], token: str) -> Tuple[List[dict], bool]:
    """
    Takes a list of records and queries the database to see if they have been used yet.

    Arguments:
        records {List[dict]} -- List of record objects.
        token {str} -- JWT

    Returns:
        Tuple[List[dict], bool] -- Returns record ids and whether they are all processed or not.
    """
    record_ids = [x["_id"] for x in records]
    query_expr = {"_id": {"$in": record_ids}}
    response = EVE_FETCHER.get(
        token=token,
        endpoint="data?where=%s" % (json.dumps(query_expr)),
    ).json()["_items"]

    # Check if all records are unprocessed.
    all_free = all(x["processed"] is False for x in response)
    return response, all_free
