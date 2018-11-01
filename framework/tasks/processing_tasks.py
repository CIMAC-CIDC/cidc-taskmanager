#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""
import json
import logging
import re
import subprocess
import time
from os import remove
from typing import List
from uuid import uuid4

import requests
from celery import group
from cidc_utils.requests import SmartFetch

from framework.celery.celery import APP
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.tasks.data_classes import RecordContext
from framework.tasks.process_npx import (mk_error, process_clinical_metadata,
                                         process_olink_npx)
from framework.tasks.variables import EVE_URL

HAPLOTYPE_FIELD_NAMES = [
    "allele_group",
    "hla_allele",
    "synonymous_mutation",
    "non_coding_mutation",
]


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
            {"trial": context.trial, "assay": context.assay, "record_id": context.record}
        )


def process_hla_file(hla_path: str, context: RecordContext) -> dict:
    """
    Turns an .hla file into an array of HLA mongo entries.

    Arguments:
        hla_path {str} -- Path to input file.
        context {RecordContext} -- Context object containing assay/trial/parentID.

    Returns:
        [dict] -- Array of HLA mongo records.
    """
    hla_records = []
    with open(hla_path, "r", 8192) as hla:
        for line in hla:
            # Split into columns
            columns = line.split()
            try:
                hla_record = {
                    "gene_name": columns[0],
                    "haplotypes": [],
                    "assay": context.assay,
                    "trial": context.trial,
                    "record_id": context.record,
                }
                for i in range(1, len(columns)):
                    haplotype = columns[i]
                    haplotype_fields = haplotype.split("_")
                    haplotype_record = {}

                    # Check for suffix.
                    if len(haplotype_fields) > 1:
                        # If suffix is found, store value, then trim.
                        if not str.isdigit(haplotype_fields[-1][-1]):
                            haplotype_record["suffix"] = haplotype_fields[-1][-1]
                            haplotype_fields[-1] = haplotype_fields[-1][:-1]

                    # Add all parts of the record that are present.
                    for j in range(2, len(haplotype_fields)):
                        haplotype_record[HAPLOTYPE_FIELD_NAMES[j - 2]] = int(
                            haplotype_fields[j]
                        )

                    hla_record["haplotypes"].append(haplotype_record)

                hla_records.append(hla_record)
            except IndexError:
                logging.error(
                    {
                        "message": "There was a problem with the format of the HLA file",
                        "category": "ERROR-CELERY-PROCESSING",
                    },
                    exc_info=True,
                )
            except ValueError:
                logging.error(
                    {
                        "message": "Attempted to convert a string to an int",
                        "category": "ERROR-CELERY-PROCESSING",
                    },
                    exc_info=True,
                )
    return hla_records


def process_table(path: str, context: RecordContext) -> dict:
    """
    Processes any table format data assuming the first row is a header row.

    Arguments:
        path {str} -- Path to file.
        context {RecordContext} -- Context object containing assay/trial/parentID.

    Raises:
        IndexError -- [description]

    Returns:
        [dict] -- List of entries, where each row becomes a mongo record.
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


def process_rsem(path: str, context: RecordContext) -> dict:
    """
    Processes an rsem type table.

    Arguments:
        path {str} -- Path to file.
        context {RecordContext} -- Context object containing assay/trial/parentID.

    Raises:
        IndexError -- [description]

    Returns:
        [dict] -- List of entries, where each row becomes a mongo record.
    """
    first_line = False
    entries = []
    with open(path, "r", 8192) as table:
        column_headers = []
        for line in table:
            if not first_line:
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
                        (
                            column_headers[i],
                            values[i].strip().replace('"', "").split(",")
                            if column_headers[i] == "transcript_id(s)"
                            else values[i].strip().replace('"', ""),
                        )
                        for i, j in enumerate(values)
                    )
                )

    add_record_context(entries, context)
    return entries


# This is a dictionary of all the currently supported filetypes for storage in
# MongoDB, with 're' indicating the regex to identify them from their
# filename, 'func' being the function used to create the mongo object and
# the key indicating the API endpoint the records are posted to.

PROC = {
    "hla": {"re": r"[._]hla[._]", "func": process_hla_file},
    "vcf": {"re": r".maf$", "func": process_table},
    "neoantigen": {
        "re": r".combined.all.binders.txt.annot.txt.clean.txt$",
        "func": process_table,
    },
    "purity": {"re": r"^Facets_output.", "func": process_table},
    "clonality_cluster": {"re": r"^cluster.tsv$", "func": process_table},
    "loci": {"re": r"^loci.tsv$", "func": process_table},
    "confints_cp": {"re": r"^sample_confints_CP.", "func": process_table},
    "pyclone": {"re": r".pyclone.tsv$", "func": process_table},
    "cnv": {"re": r"_segments", "func": process_table},
    "rsem_expression": {"re": r"_expression.genes$", "func": process_rsem},
    "rsem_isoforms": {"re": r"_expression.isoforms$", "func": process_table},
    "olink": {"re": r"olink.*npx", "func": process_olink_npx},
    "olink_meta": {"re": r"olink.*biorepository", "func": process_clinical_metadata},
}


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
    """[summary]

    Arguments:
        response {dict} -- [description]
        records {List[dict]} -- [description]

    Returns:
        List[dict] -- [description]
    """
    invalid_records = (
        response.json()["_items"] if "_items" in response.json() else [response.json()]
    )
    new_upload = []
    for validation_error, record in zip(invalid_records, records):
        if "validation_errors" in record:
            record["validation_errors"].append(
                mk_error(
                    "Record failed datatype validation: %s"
                    % json.dumps(validation_error["_issues"]),
                    affected_paths=[],
                )
            )
            new_upload.append(
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
    Adds item to parent record's child list.

    Arguments:
        record {dict} -- [description]
        endpoint {str} -- [description]

    Returns:
        dict -- [description]
    """
    fetcher = SmartFetch(EVE_URL)
    query = {"_id": parent_id}

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
        parent = fetcher.get(
            endpoint="data?where=%s" % json.dumps(query),
            token=process_file.token["access_token"],
        ).json()

        # Update parent record.
        res = requests.post(
            EVE_URL + "/data_edit/%s" % str(parent["_items"][0]["_id"]),
            json={"children": parent["_items"][0]["children"] + new_children},
            headers={
                "If-Match": parent["_items"][0]["_etag"],
                "Authorization": "Bearer {}".format(process_file.token["access_token"]),
                "X-HTTP-Method-Override": "PATCH",
            },
        )
        if not res.status_code == 200:
            raise RuntimeError

        return res
    except RuntimeError:
        if parent.status_code != 200:
            logging.error(
                {"message": "Error fetching parent", "category": "ERROR-CELERY-GET"}
            )
        if res and res.status_code != 200:
            logging.error(
                {
                    "message": "Error updating child list of parent",
                    "category": "ERROR-CELERY-PATCH-FAIR",
                }
            )
        return None


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
    eve_fetcher = SmartFetch(EVE_URL)
    temp_file_name = str(uuid4())
    gs_args = ["gsutil", "cp", rec["gs_uri"], temp_file_name]
    subprocess.run(gs_args)
    records = None
    response = None

    try:
        # Use a random filename as tasks are executing in parallel.
        records = PROC[pro]["func"](
            temp_file_name,
            RecordContext(
                rec["trial"]["$oid"], rec["assay"]["$oid"], rec["_id"]["$oid"]
            ),
        )
        remove(temp_file_name)
    except OSError:
        pass

    try:
        # First try a normal upload
        response = eve_fetcher.post(
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
    except RuntimeError:
        # Catch unexpected error codes.
        if not response.status_code == 422:
            logging.error(
                {
                    "message": "Upload failed for reasons other than validation.",
                    "category": "ERROR-CELERY-UPLOAD",
                }
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
            errors = eve_fetcher.post(
                endpoint=pro,
                token=process_file.token["access_token"],
                code=201,
                json=new_upload,
            )

            # Still update parent to tie to error documents.
            update_child_list(errors.json(), pro, rec["_id"]["$oid"])

            log_record_upload(new_upload, pro)
        except RuntimeError:
            # If that fails, something on my end is wrong.
            logging.error(
                {
                    "message": "Total failure of biomarker upload.",
                    "category": "ERROR-CELERY-UPLOAD",
                }
            )
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
                log = "Match found for " + rec["file_name"]
                logging.info({"message": log, "category": "DEBUG-CELERY-PROCESSING"})
                # If a match is found, add to job queue
                tasks.append(process_file.s(rec, pro))

    job = group(tasks) if len(tasks) == 1 else group(*tasks)
    result = job.apply_async()

    for task in tasks:
        info = "Task: " + task.name + " is now starting"
        logging.info({"message": info, "category": "DEBUG-CELERY-PROCESSING"})

    # Wait for jobs to finish.
    cycles = 0
    while not result.ready() and cycles < 70:
        time.sleep(10)
        cycles += 1

    if not result.successful():
        logging.error(
            {
                "message": "Error, some of the tasks failed",
                "category": "ERROR-CELERY-PROCESSING",
            }
        )
