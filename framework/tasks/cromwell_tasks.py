#!/usr/bin/env python3
"""
Celery tasks for handling some basic interaction with uploaded files.
"""
import datetime
import logging
import subprocess
from typing import List
from uuid import uuid4

from cidc_utils.requests import SmartFetch

from framework.celery.celery import APP
from framework.tasks.administrative_tasks import manage_bucket_acl, get_authorized_users
from framework.tasks.AuthorizedTask import AuthorizedTask
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
    staging_id = upload_record["_id"]
    files = upload_record["files"]

    for record in files:
        record["gs_uri"] = record["gs_uri"] = (
            google_path
            + record["trial"]["$oid"]
            + "/"
            + record["assay"]["$oid"]
            + "/"
            + str(uuid4())
            + "/"
            + record["file_name"]
        )
        record["date_created"] = str(datetime.datetime.now().isoformat())
        old_uri = (
            "gs://" + GOOGLE_UPLOAD_BUCKET + "/" + staging_id["$oid"] + "/" + record["file_name"]
        )

        # Move file to destination.
        record["trial"] = record["trial"]["$oid"]
        record["assay"] = record["assay"]["$oid"]
        gs_args = ["gsutil", "mv", old_uri, record["gs_uri"]]
        run_subprocess_with_logs(gs_args, "Moving Files: ")
        log = "Moved record: %s from %s to %s" % (
            record["file_name"],
            old_uri,
            record["gs_uri"],
        )
        logging.info({"message": log, "category": "FAIR-CELERY-RECORD"})
        # Grant access to files in google storage.
        collabs = get_authorized_users(
            {"trial": record["trial"], "assay": record["assay"]},
            move_files_from_staging.token["access_token"],
        )
        manage_bucket_acl(GOOGLE_BUCKET_NAME, record["gs_uri"], collabs)

    # when move is completed, insert data objects
    EVE_FETCHER.post(
        token=move_files_from_staging.token["access_token"],
        json=files,
        endpoint="data_edit",
        code=201,
    )
