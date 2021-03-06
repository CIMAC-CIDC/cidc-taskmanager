#!/usr/bin/env python
"""
Module for tasks that do post-run processing of output files.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import datetime
import json
import logging
import re
import subprocess
import time
from os import remove
from typing import List
from uuid import uuid4

from cidc_utils.requests import SmartFetch
from cidc_utils.loghandler.stack_driver_handler import log_formatted
from dateutil import parser

from framework.celery.celery import APP
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.data_classes import RecordContext
from framework.tasks.parallelize_tasks import execute_in_parallel
from framework.tasks.process_npx import (
    mk_error,
    process_clinical_metadata,
    process_olink_npx,
)
from framework.tasks.storage_tasks import run_subprocess_with_logs
from framework.tasks.variables import EVE_URL

EVE_FETCHER = SmartFetch(EVE_URL)


def add_record_context(records: List[dict], context: RecordContext) -> None:
    """
    Adds the trial, assay, and record ID to a processed record.

    Arguments:
        records {List[dict]} -- List of records.
        context {RecordContext} -- Context object containing assay/trial/parentID.

    Returns:
        None -- [description]
    """
    for record in records:
        record.update(
            {
                "trial": context.trial,
                "assay": context.assay,
                "record_id": context.record,
            }
        )


def process_table(path: str, context: RecordContext) -> List[dict]:
    """
    Processes any table format data assuming the first row is a header row.

    Arguments:
        path {str} -- Path to file.
        context {RecordContext} -- Context object containing assay/trial/parentID.

    Raises:
        IndexError -- Will be thrown if there is some mismatch number of headers and number of
            values in a row

    Returns:
        List[dict] -- List of entries, where each row becomes a mongo record.
    """
    first_line = False
    entries = []
    with open(path, "r", 8192) as table:
        column_headers = []
        for line in table:
            if line[0] != "#" and not first_line:
                first_line = True
                column_headers = [
                    header.strip().replace('"', "").replace(".", "")
                    for header in line.split("\t")
                ]
            elif first_line:
                values = line.split("\t")
                if not len(column_headers) == len(values):
                    logging.error(
                        {
                            "message": "Header and value length mismatch!",
                            "category": "ERROR-CELERY-PROCESSING",
                        }
                    )
                    raise IndexError
                entries.append(
                    dict(
                        (column_headers[i], values[i].strip().replace('"', ""))
                        for i, j in enumerate(values)
                    )
                )

    add_record_context(entries, context)
    return entries


def reformat_maf(new_record: dict, context: RecordContext) -> dict:
    """
    Deletes some items to turn a MAF into a combined maf.

    Arguments:
        new_record {dict} -- Data record for MAF file.
        context {RecordContext} -- Object holding the fetched maf file.

    Returns:
        dict -- Reformatted MAF record.
    """
    # Rename and create a new data entry.
    new_record["file_name"] = "combined.maf"

    # Depending on the circumstances of the call, these extra keys may or may not exist.
    for key in {"_id", "_etag", "_created", "_updated", "_links", "_status"}:
        try:
            new_record.__delitem__(key)
        except KeyError:
            pass
    new_record["trial"] = context.trial
    new_record["assay"] = context.assay
    new_record["processed"] = True

    # Generate new alias for combined maf.
    new_record["gs_uri"] = (
        new_record["gs_uri"].replace(new_record["file_name"], "") + "combined.maf"
    )
    return new_record


def combine_mafs(path: str, combined_file_name: str) -> None:
    """
    Takes an existing maf, then writes a second maf file to it.

    Arguments:
        path {str} -- Path to existing combined maf.
        combined_file_name {str} -- Path to write out to.

    Returns:
        None -- [description]
    """
    with open(combined_file_name, "a") as outfile, open(path, "r") as new_maf:
        line = new_maf.readline()
        while line[0] == "#":
            line = new_maf.readline()
        while line:
            outfile.write(new_maf.readline())
            line = new_maf.readline()


def create_new_maf(path: str, context: RecordContext, token: str) -> bool:
    """
    Create a new combined MAF.

    Arguments:
        path {str} -- path to file
        context {RecordContext} -- Object holding the record.

    Returns:
        bool -- True if succeeds, else false.
    """
    new_record = context.full_record

    # Check for other analyses that finished recently.
    an_query = "analysis?where=%s" % json.dumps(
        {"trial": context.trial, "assay": context.assay, "status": "Completed"}
    )
    analysis_runs = EVE_FETCHER.get(endpoint=an_query, token=token).json()["_items"]
    isotime = datetime.datetime.now(datetime.timezone.utc)

    my_end = 0
    my_run_id = None
    for run in analysis_runs:
        for output in run["files_generated"]:
            if output["file_name"] == new_record["file_name"]:
                my_end = parser.parse(run["end_date"])
                my_run_id = run["_id"]

    # If the time is close enough that the trial might not have cleaned up yet,
    # wait, then retry.
    for run in analysis_runs:
        end = parser.parse(run["end_date"])
        delta = (isotime - end).total_seconds()
        if run["_id"] != my_run_id and delta < 30:
            log_formatted(
                logging.info,
                "Another run ended recently, running checks to avoid race condition.",
                "INFO-CELERY-MAF",
            )
            if (
                my_end > end
                or my_end == end
                and min(my_run_id, run["_id"]) == my_run_id
            ):
                log_formatted(
                    logging.info,
                    "Detecting an older finished run. Sleeping",
                    "INFO-CELERY-SLEEP",
                )
                time.sleep(5)
                process_maf(path, context)
                return True

    # Rename and create a new data entry.
    reformat_maf(new_record, context)

    # Generate new alias for combined maf.
    new_record["gs_uri"] = (
        new_record["gs_uri"].replace(new_record["file_name"], "") + "/combined.maf"
    )
    run_subprocess_with_logs(
        ["gsutil", "mv", path, new_record["gs_uri"]],
        message="Copying new combined.maf to google bucket: %s" % new_record["gs_uri"],
    )

    try:
        remove(path)
    except FileNotFoundError:
        pass

    log_formatted(
        logging.info,
        "Copying for new combined.maf: %s complete." % new_record["gs_uri"],
        "FAIR-CELERY-NEWCOMBINEDMAF",
    )

    try:
        EVE_FETCHER.post(endpoint="data_edit", token=token, json=new_record, code=201)
        return True
    except RuntimeError as rte:
        log_formatted(
            logging.error,
            "Failed to create new combined.maf: %s" % str(rte),
            "ERROR-CELERY-POST",
        )
        return False


def process_maf(path: str, context: RecordContext) -> bool:
    """
    Processes a new maf file to update the combined maf.

    Arguments:
        path {str} -- On disk path of the file.
        context {RecordContext} -- Class holding trial, assay, and a copy of the record.

    Returns:
        bool -- True if finished without error, else false.
    """
    if context.full_record["file_name"] == "combined.maf":
        return True

    token = process_file.token["access_token"]
    # Check if a combined maf already exists.
    query_string = "data?where=%s" % json.dumps(
        {"trial": context.trial, "assay": context.assay, "file_name": "combined.maf"}
    )

    extant_combined_maf = EVE_FETCHER.get(endpoint=query_string, token=token).json()[
        "_items"
    ]

    if not extant_combined_maf:
        create_new_maf(path, context, token)
        return True

    combined_maffile = extant_combined_maf[0]
    maf_gs_uri = combined_maffile["gs_uri"]
    combined_file_name = str(uuid4())
    run_subprocess_with_logs(
        ["gsutil", "mv", maf_gs_uri, combined_file_name],
        message="Copying combined maf file %s" % maf_gs_uri,
    )

    # Combine mafs.
    combine_mafs(path, combined_file_name)

    # Overwrite old file.
    run_subprocess_with_logs(
        ["gsutil", "mv", combined_file_name, maf_gs_uri],
        message="Overwriting old combined.maf: %s" % maf_gs_uri,
    )
    log_formatted(
        logging.info,
        "Overwriting old combined.maf: %s" % maf_gs_uri,
        "FAIR-CELERY-COMBINEDMAF",
    )
    new_sample_ids = combined_maffile["sample_ids"] + context.full_record["sample_ids"]

    # Patch data to include new samples.
    try:
        EVE_FETCHER.patch(
            endpoint="data_edit",
            item_id=combined_maffile["_id"],
            _etag=combined_maffile["_etag"],
            json={
                "sample_ids": new_sample_ids,
                "number_of_samples": len(new_sample_ids),
            },
            token=token,
        )
        return True
    except RuntimeError as rte:
        if "412" in str(rte):
            time.sleep(5)
            process_maf(path, context)
            return True

        log_formatted(
            logging.error,
            "Failed to edit combined.maf: %s" % str(rte),
            "ERROR-CELERY-PATCH",
        )
        return False


def log_record_upload(records: List[dict], endpoint: str) -> None:
    """
    Takes a list of records, and the endpoint they are being sent to, and logs the fact that
    they were uploaded.

    Arguments:
        records {List[dict]} -- List of mongo records.
        endpoint {str} -- API endpoint they are sent to.

    Returns:
        None -- [description]
    """
    for record in records:
        log = "Record: %s added to collection: %s on trial: %s on assay: %s" % (
            record["file_name"] if "file_name" in record else " ",
            endpoint,
            record["trial"],
            record["assay"],
        )
        logging.info({"message": log, "category": "FAIR-CELERY-RECORD"})


def report_validation_issues(response: dict, records: List[dict]) -> List[dict]:
    """
    If a document fails to be uploaded due to failing schema validation, upload the validation
    errors so the user can see why.

    Arguments:
        response {dict} -- Response object from POST.
        records {List[dict]} -- List of records.

    Returns:
        List[dict] -- Returns a list of formatted errors.
    """
    invalid_records = (
        response.json()["_items"] if "_items" in response.json() else [response.json()]
    )
    new_upload = []
    append_to_nu = new_upload.append

    for validation_error, record in zip(invalid_records, records):
        if "validation_errors" in record:
            record["validation_errors"].append(
                mk_error(
                    "Record failed datatype validation: %s"
                    % json.dumps(validation_error["_issues"]),
                    affected_paths=[],
                )
            )
            append_to_nu(
                {
                    "assay": record["assay"],
                    "trial": record["trial"],
                    "record_id": record["record_id"],
                    "validation_errors": record["validation_errors"],
                }
            )
    return new_upload


def update_child_list(record_response: dict, endpoint: str, parent_id: str) -> dict:
    """
    Adds item to a record's child list.

    Arguments:
        record_response {dict} -- Response to POST of new child.
        endpoint {str} -- Resource endpoint of record.
        parent_id {str} -- id of parent record.

    Raises:
        RuntimeError -- [description]

    Returns:
        dict -- HTTP response to patch.
    """
    records = None

    if "_items" in record_response:
        records = record_response["_items"]
    else:
        records = [record_response]

    new_children = []
    for record in records:
        new_children.append({"_id": record["_id"], "resource": endpoint})

    try:
        # Get etag of parent.
        parent = EVE_FETCHER.get(
            endpoint="data?where=%s" % json.dumps({"_id": parent_id}),
            token=process_file.token["access_token"],
        ).json()

        # Update parent record.
        return EVE_FETCHER.patch(
            endpoint="data_edit",
            item_id=str(parent["_items"][0]["_id"]),
            json={"children": parent["_items"][0]["children"] + new_children},
            _etag=parent["_items"][0]["_etag"],
            token=process_file.token["access_token"],
        )
    except RuntimeError as code_error:
        if not parent:
            log_formatted(
                logging.error,
                "Error fetching parent %s" % str(code_error),
                "ERROR-CELERY-GET",
            )
        else:
            log_formatted(
                logging.error,
                "Error updating child list of parent: %s" % str(code_error),
                "ERROR-CELERY-PATCH-FAIR",
            )
        return None


# This is a dictionary of all the currently supported filetypes for storage in
# MongoDB, with 're' indicating the regex to identify them from their
# filename, 'func' being the function used to create the mongo object and
# the key indicating the API endpoint the records are posted to. The mongo key
# indicates whether or not the filetype should be converted to mongo records.
PROC = {
    "olink": {"re": r"olink.*npx", "func": process_olink_npx, "mongo": True},
    "olink_meta": {
        "re": r"olink.*biorepository",
        "func": process_clinical_metadata,
        "mongo": True,
    },
    "maf": {"re": r".maf$", "func": process_maf, "mongo": False},
}


@APP.task(base=AuthorizedTask)
def process_file(rec: dict, pro: str) -> bool:
    """
    Worker process that handles processing an individual file.

    Arguments:
        rec {dict} -- A record item to be processed.
        pro {str} -- Key in the processing dictionary that the filetype
        corresponds to.

    Returns:
        boolean -- True if completed without error, else false.
    """
    temp_file_name = str(uuid4())
    gs_args = ["gsutil", "cp", rec["gs_uri"], temp_file_name]
    subprocess.run(gs_args)
    records = None
    response = None

    try:
        # Use a random filename as tasks are executing in parallel.
        if PROC[pro]["mongo"]:
            records = PROC[pro]["func"](
                temp_file_name,
                RecordContext(
                    rec["trial"]["$oid"], rec["assay"]["$oid"], rec["_id"]["$oid"]
                ),
            )
        else:
            PROC[pro]["func"](
                temp_file_name,
                RecordContext(
                    rec["trial"]["$oid"], rec["assay"]["$oid"], rec["_id"]["$oid"], rec
                ),
            )
        remove(temp_file_name)
    except OSError:
        pass

    if not PROC[pro]["mongo"]:
        return True

    if not records:
        return False

    try:
        # First try a normal upload
        response = EVE_FETCHER.post(
            endpoint=pro,
            token=process_file.token["access_token"],
            code=201,
            json=records,
        )

        update_child_list(response.json(), pro, rec["_id"]["$oid"])

        # Simplify handling later
        if not isinstance(records, (list,)):
            records = [records]

        # Record uploads.
        log_record_upload(records, pro)
        return True
    except RuntimeError as rte:
        # Catch unexpected error codes.
        if "422" not in str(rte):
            log_formatted(
                logging.error,
                "Upload failed for reasons other than validation: %s" % str(rte),
                "ERROR-CELERY-UPLOAD",
            )
            return False

        # Extract Cerberus errors from response.
        new_upload = report_validation_issues(response, records)
        if not new_upload:
            # Handle data types that haven't used the new schema yet.
            logging.warning(
                {
                    "message": (
                        "Validation error detected in upload, but schema not adapted to"
                        "handle error reports"
                    ),
                    "category": "WARNING-CELERY-UPLOAD",
                }
            )
            return False
        try:
            # Try to upload just the errors.
            errors = EVE_FETCHER.post(
                endpoint=pro,
                token=process_file.token["access_token"],
                code=201,
                json=new_upload,
            )

            # Still update parent to tie to error documents.
            update_child_list(errors.json(), pro, rec["_id"]["$oid"])
            log_record_upload(new_upload, pro)
        except RuntimeError as rte:
            # If that fails, something on my end is wrong.
            log = "Upload of validation errors failed. %s" % str(rte)
            logging.error({"message": log, "category": "ERROR-CELERY-UPLOAD"})
    return False


@APP.task
def postprocessing(records: List[dict]) -> None:
    """
    Scans incoming records and sees if they need post-processing.
    If so, they are processed and then uploaded to mongo.

    Arguments:
        records {List[dict]} -- List of records slated for processing.
    """
    tasks = []

    for rec in records:
        message = "Processing: " + rec["file_name"]
        logging.info({"message": message, "category": "FAIR-CELERY-PROCESSING"})
        for pro in PROC:
            if re.search(PROC[pro]["re"], rec["file_name"], re.IGNORECASE):
                log_formatted(
                    logging.info,
                    "Match found for " + rec["file_name"],
                    "INFO-CELERY-PROCESSING",
                )
                # If a match is found, add to job queue
                tasks.append(process_file.s(rec, pro))

    execute_in_parallel(tasks)
