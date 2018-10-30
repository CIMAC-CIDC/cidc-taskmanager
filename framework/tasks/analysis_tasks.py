#!/usr/bin/env python3
"""
Simple celery example
"""
import datetime
import json
import logging
import re
import time
from typing import List, Tuple

import requests
from cidc_utils.requests import SmartFetch

from framework.celery.celery import APP
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.tasks.cromwell_tasks import manage_bucket_acl
from framework.tasks.variables import CROMWELL_URL, EVE_URL

EVE_FETCHER = SmartFetch(EVE_URL)
CROMWELL_FETCHER = SmartFetch(CROMWELL_URL)


def get_run_log(run_id: str) -> dict:
    """
    Retrieves the logs for the given run ID.

    Arguments:
        run_id {str} -- Mongo ID of run.

    Raises:
        RuntimeError -- [description]

    Returns:
        dict -- Log data.
    """
    endpoint = run_id + "/logs"
    log_data = CROMWELL_FETCHER.get(endpoint=endpoint).json()
    return json.dumps(log_data)


def add_meta_item(analysis_info: dict, entry: dict) -> dict:
    """
    Packages cromwell output into a mongo entry

    Arguments:
        analysis_info {dict} -- Dictionary containing run information, specifically emails and
            data records.
        entry {dict} -- Key-value pair.

    Returns:
        dict -- [description]
    """
    record = analysis_info["record"]
    (key, value), = entry.items()
    manage_bucket_acl("lloyd-test-pipeline", value, analysis_info["emails"])

    return {
        "file_name": key,
        "gs_uri": value,
        "sample_id": record["samples"][0],
        "trial": record["trial"],
        "assay": record["assay"],
        "analysis_id": record["analysis_id"],
        "date_created": str(datetime.datetime.utcnow().isoformat()),
        "mapping": key.split(".", 1)[1],
    }


def meta_parse(
    analysis_info: dict, parent_key: str, item: object, filegen: List[dict]
) -> None:
    """
    Takes a metadata output JSON from a cromwell run, and returns a list of records
    formatted for the database. Function has recursion to handle nested structures.
    Also updates the access control lists of the objects affected.

    Arguments:
        analysis_info {dict} -- Run information.
        parent_key {str} -- Key under which the entry is listed.
        item {object} -- The output json value being processed.
        filegen {List[dict]} -- List of files being handled.
    """
    if isinstance(item, str):
        fil = add_meta_item(analysis_info, {parent_key: item})
        filegen.append(fil)
    elif isinstance(item, list):
        for sub_item in item:
            meta_parse(analysis_info, parent_key, sub_item, filegen)
    elif isinstance(item, dict):
        for key, value in item.items():
            meta_parse(analysis_info, key, value, filegen)


def create_analysis_entry(analysis_info: dict, token: str) -> dict:
    """
    Updates the analysis entry with the information from the completed run.

    Arguments:
        analysis_info {dict} -- Information on the run.
        _etag {str} -- Etag value for patching.
        token {str} -- Access token.

    Returns:
        dict -- response data from the API.
    """
    payload_object = {
        "files_generated": [],
        "status": {
            "progress": "Completed" if analysis_info["status"] else "Aborted",
            "message": "",
        },
    }

    # If run failed, append logs to record.
    if not analysis_info["status"]:
        payload_object["status"]["message"] = get_run_log(analysis_info["run_id"])

    metadata = CROMWELL_FETCHER.get(
        endpoint=analysis_info["run_id"] + "/metadata"
    ).json()
    filegen = payload_object["files_generated"]

    # Parse file string to JSON
    string_meta = json.dumps(metadata)
    payload_object["metadata_blob"] = string_meta

    pattern = re.compile(r"gs://")
    for key, value in metadata["outputs"].items():
        if re.search(pattern, str(value)):
            meta_parse(analysis_info, key, value, filegen)

    # Insert the analysis object
    patch_res = requests.patch(
        EVE_URL + "/analysis/" + analysis_info["record"]["analysis_id"],
        json=payload_object,
        headers={"If-Match": analysis_info["_etag"], "Authorization": "Bearer {}".format(token)},
    )

    if not patch_res.status_code == 200:
        log = "Error communicating with eve: %s" % patch_res.reason
        logging.error({"message": log, "category": "ERROR-CELERY"})
        raise RuntimeError

    # Insert data
    if filegen:
        return EVE_FETCHER.post(
            token=token,
            endpoint="data",
            json=[
                {"file_name": entry["file_name"], "gs_uri": entry["gs_uri"]}
                for entry in filegen
            ],
            code=201,
        )
    return None


def check_for_runs() -> Tuple[requests.Response, requests.Response]:
    """
    Queries data collection and compares with assay requirements.
    If any runs are ready, sends out appropriate data.

    Returns:
        requests.Response -- Response containing data.
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
        assay_response: [{
            static_inputs: [
                {
                    key_name: "...",
                    key_value: "..."
                }
            ],
            non_static_inputs: ["...","..."],
            wdl_location: "..."
        }]
    """
    assay_projection = {"non_static_inputs": 1, "static_inputs": 1, "wdl_location": 1}

    # Filter out any assay that does not have a wdl associated with it.
    pipeline_filter = {"wdl_location": {"$ne": "null"}}

    assay_query_string = "assays?where=%s&projection=%s" % (
        json.dumps(pipeline_filter),
        json.dumps(assay_projection),
    )

    # Contains a list of all the running assays and their inputs
    assay_response = EVE_FETCHER.get(
        token=analysis_pipeline.token["access_token"],
        endpoint=assay_query_string,
        code=200,
    ).json()["_items"]

    # Creates a list of all sought mappings to find user-defined inputs
    sought_mappings = [
        item
        for sublist in [x["non_static_inputs"] for x in assay_response]
        for item in sublist
    ]

    # For stage N > 2 pipelines, the inputs need to be computed from the trailing file names
    # of the outputs, so the string must be parsed to cut off the run name unlike the
    # user-uploaded files which can be matched against the whole name.
    input_names = [
        item.split(".", 1)[1] if len(item.split(".", 1)) == 2 else item
        for sublist in [x["non_static_inputs"] for x in assay_response]
        for item in sublist
    ]

    # Query the data and organize it into groupings
    # Add together the mapping and input names as a search
    # The data are grouped into unique groups via trial/assay/sample_id
    aggregate_query = {"$inputs": sought_mappings + input_names}

    query_string = "data/query?aggregate=%s" % (json.dumps(aggregate_query))

    try:
        record_response = EVE_FETCHER.get(
            token=analysis_pipeline.token["access_token"], endpoint=query_string
        )
        return record_response, assay_response
    except RuntimeError:
        error_msg = "Failed to fetch record: %s. %s" % (
            record_response.reason,
            record_response.status_code,
        )
        logging.error({"message": error_msg, "category": "ERROR-CELERY-QUERY"})


def create_input_json(sample_assay: dict, assay: dict) -> dict:
    """
    Constructs the input.json file to run the pipeline.

    Arguments:
        sample_assay {dict} -- Record representing a group of files with the same sampleID
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


def set_record_processed(records: List[dict], condition: bool) -> bool:
    """
    Takes a list of records, then changes their
    processed status to match the condition

    Arguments:
        records {[dict]} -- List of "data" collection records.
        condition {bool} -- True if processed else false.

    Returns:
        bool -- True if all records were succesfully patched, else false.
    """
    patch_status = []
    for record in records:
        patch_res = requests.patch(
            EVE_URL + "/data/" + record["_id"],
            json={"processed": condition},
            headers={
                "If-Match": record["_etag"],
                "Authorization": "Bearer {}".format(
                    analysis_pipeline.token["access_token"]
                ),
            },
        )
        patch_status.append(patch_res.status_code == 200)

    return all(patch_status)


def check_processed(records: List[dict]) -> Tuple[List[dict], bool]:
    """
    Takes a list of records and queries the database to see if they have been used yet.

    Arguments:
        records {[dict]} -- List of record objects.

    Returns:
        Tuple -- Returns record ids and whether they are all processed or not.
    """
    record_ids = [x["_id"] for x in records]
    query_expr = {"_id": {"$in": record_ids}}
    response = EVE_FETCHER.get(
        token=analysis_pipeline.token["access_token"],
        endpoint="data?where=%s" % (json.dumps(query_expr)),
    ).json()["_items"]

    # Check if all records are unprocessed.
    all_free = all(x["processed"] is False for x in response)
    return response, all_free


def start_cromwell_flows(assay_response: List[dict], groups: List[dict]):
    """
    Checks through all of the available data, and if appropriate,
    begins pipeline runs on cromwell.

    Arguments:
        assay_response {[dict]} -- Response from assay endpoint.
        groups {[dict]} -- Result of aggregator query, has records grouped
        by sample ID along with assay/trial.

    Returns:
        response_arr: {[dict]} -- All of the responses from the cromwell API,
        so that the runs can be tracked for completion.
    """
    # For each registered trial/assay, check if there is data that can be run.
    response_arr = []
    for assay in assay_response:

        # Get list of trials that include the assay.
        query = {"assays.assay_id": assay["_id"]}
        trials = EVE_FETCHER.get(
            endpoint="trials?where=%s" % (json.dumps(query)),
            token=analysis_pipeline.token["access_token"],
            code=200,
        ).json()["_items"]

        # Make array of IDs
        trial_ids = [x["_id"] for x in trials]

        # Make sure that the trial the data belongs to includes the assay about to be run.
        for sample_assay in groups:
            if sample_assay["_id"]["trial"] in trial_ids and len(
                sample_assay["records"]
            ) == len(assay["non_static_inputs"]):
                if not "sample_id" in sample_assay["_id"]:
                    continue

                payload = {
                    "trial": str(sample_assay["_id"]["trial"]),
                    "assay": str(assay["_id"]),
                    "samples": [str(sample_assay["_id"]["sample_id"])],
                }

                # Query all records to make sure not processed
                records = sample_assay["records"]
                record_ids, all_free = check_processed(records)

                if not all_free:
                    message = "Record involved in run has already been used" + str(
                        sample_assay["_id"]["trial"]
                    )
                    logging.warning(
                        {"message": message, "category": "WARNING-CELERY-PIPELINES"}
                    )
                    set_record_processed(record_ids, False)
                    return []

                # Patch all to ensure atomicity
                if not set_record_processed(record_ids, True):
                    message = "Patch operation failed! Aborting!" + str(
                        sample_assay["_id"]["trial"]
                    )
                    logging.error(
                        {"message": message, "category": "ERROR-CELERY-PIPELINE"}
                    )
                    set_record_processed(record_ids, False)
                    return []

                # Create analysis entry.
                res_json = EVE_FETCHER.post(
                    token=analysis_pipeline.token["access_token"],
                    endpoint="analysis",
                    json=payload,
                    code=201,
                ).json()
                payload["analysis_id"] = res_json["_id"]

                # Create inputs.json file.
                files = {
                    "workflowSource": open(assay["wdl_location"], "rb"),
                    "workflowInputs": json.dumps(
                        create_input_json(sample_assay, assay)
                    ).encode("utf-8"),
                }

                response_arr.append(
                    {
                        "api_response": CROMWELL_FETCHER.post(
                            code=201, files=files
                        ).json(),
                        "analysis_etag": res_json["_etag"],
                        "record": payload,
                    }
                )

    return response_arr


@APP.task(base=AuthorizedTask)
def analysis_pipeline():
    """
    Start of a universal "check ready" scraper.
    """
    # Check if any runs are ready.
    record_response, assay_response = check_for_runs()

    # If they are, send them to cromwell to start.
    active_runs = start_cromwell_flows(assay_response, record_response.json()["_items"])

    # Poll for active runs until completed.
    while active_runs:
        time.sleep(5)
        message = str(len(active_runs)) + " runs still active..."
        logging.info({"message": message, "category": "DEBUG-CELERY"})
        filter_runs = []

        for run in active_runs:
            run_id = run["api_response"]["id"]
            record = run["record"]

            # Poll status.
            endpoint = run_id + "/" + "status"
            response = CROMWELL_FETCHER.get(endpoint=endpoint, code=200).json()[
                "status"
            ]

            # If run has finished, make a record.
            if response not in ["Submitted", "Running"]:
                create_analysis_entry(
                    {
                        "record": record,
                        "run_id": run_id,
                        "status": response == "Succeeded",
                        "_etag": run["analysis_etag"],
                    },
                    analysis_pipeline.token["access_token"],
                )
                # Add run to filter list.
                filter_runs.append(run)

        # Filter finished runs out.
        active_runs = [x for x in active_runs if x not in filter_runs]
