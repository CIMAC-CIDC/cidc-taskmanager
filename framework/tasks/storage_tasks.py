#!/usr/bin/env python
"""
Celery tasks for handling some basic interaction with uploaded files.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import datetime
import logging
import subprocess
from typing import List

from cidc_utils.requests import SmartFetch

from framework.celery.celery import APP
from framework.tasks.administrative_tasks import get_authorized_users, manage_bucket_acl
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.variables import EVE_URL, GOOGLE_BUCKET_NAME, GOOGLE_UPLOAD_BUCKET

EVE_FETCHER = SmartFetch(EVE_URL)


def run_subprocess_with_logs(
    cl_args: List[str], message: str, encoding: str = "utf-8", cwd: str = "."
) -> None:
    """
    Runs a subprocess command and logs the output.

    Arguments:
        cl_args {List[str]} -- List of string inputs to the shell command.
        message {str} -- Message that will precede output in the log.
        encoding {str} -- indicates the encoding of the shell command output.
        cwd {str} -- Current working directory.
    """
    try:
        logging.info({"message": message, "category": "DEBUG"})
        subprocess.run(cl_args, cwd=cwd)
    except subprocess.CalledProcessError:
        logging.error(
            {"message": "Subprocess failed", "category": "ERROR-CELERY"}, exc_info=True
        )


@APP.task(base=AuthorizedTask)
def move_files_from_staging(upload_record: dict, google_path: str) -> None:
    """
    Function that moves a file from staging to permanent storage

    Decorators:
        APP

    Arguments:
        upload_record {dict} -- Ingestion collection record listing files to be uploaded.
        google_path {str} -- Path to storage bucket.
    """

    staging_id = upload_record["_id"]["$oid"]

    for record in upload_record["files"]:
        trial_id = record["trial"]["$oid"]
        assay_id = record["assay"]["$oid"]
        record["trial"] = trial_id
        record["assay"] = assay_id
        record["date_created"] = str(
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        )
        record["gs_uri"] = str.join(
            "", (google_path, trial_id, "/", assay_id, "/", record["uuid_alias"])
        )

        old_name = str.join("", (staging_id, "/", record["uuid_alias"]))
        old_uri = "gs://%s/%s" % (GOOGLE_UPLOAD_BUCKET, old_name)
        gs_args = ["gsutil", "mv", old_uri, record["gs_uri"]]
        log = "Moved record: %s from %s to %s" % (
            record["file_name"],
            old_uri,
            record["gs_uri"],
        )
        run_subprocess_with_logs(gs_args, "Moving files: ")
        logging.info({"message": log, "category": "FAIR-CELERY-RECORD"})
        authorized_users = get_authorized_users(
            {"trial": record["trial"], "assay": record["assay"]},
            move_files_from_staging.token["access_token"],
        )
        manage_bucket_acl(GOOGLE_BUCKET_NAME, record["gs_uri"], authorized_users)

    try:
        EVE_FETCHER.post(
            token=move_files_from_staging.token["access_token"],
            json=upload_record["files"],
            endpoint="data_edit",
            code=201,
        )
    except RuntimeError as rte:
        log = "Error creating records for uploaded files: %s. Payload %s" % (
            str(rte),
            str(upload_record["files"]),
        )
        logging.error({"message": log, "category": "ERROR-CELERY-MOVEFILES"})
