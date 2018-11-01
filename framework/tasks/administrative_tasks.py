#!/usr/bin/env python3
"""
These tasks are responsible for performing administrative and user management tasks.
"""
import json
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from os import remove
from typing import List

import requests
from cidc_utils.requests import SmartFetch
from dateutil.parser import parse
from google.cloud import storage

from framework.celery.celery import APP
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.tasks.variables import (
    AUTH0_DOMAIN,
    EVE_URL,
    GOOGLE_BUCKET_NAME,
    LOGSTORE,
    MANAGEMENT_API,
)

EVE_FETCHER = SmartFetch(EVE_URL)


def get_user_trials(user_email: str, token: str) -> List[dict]:
    """
    Gets the ids of all trials given user is a collaborator on.

    Arguments:
        user_email {str} -- User's email.
        token {str} -- JWT for API access.

    Returns:
        [type] -- [description]
    """
    collabs = {"collaborators": user_email}
    projection = {"_id": 1}
    query = "trials?where=%s&projection=%s" % (
        json.dumps(collabs),
        json.dumps(projection),
    )
    return EVE_FETCHER.get(token=token, endpoint=query).json()["_items"]


def get_user_records(matched_trials: List[dict], token: str) -> List[str]:
    """
    Gets a list of the GCS paths for all objects the user is authorized tos ee.

    Arguments:
        matched_trials {List[dict]} -- List of objects specifying trial ids and etags.
        token {str} -- API access token.

    Returns:
        List[str] -- List of GCS paths.
    """
    trial_ids = [trial["_id"] for trial in matched_trials]
    condition = {"trial": {"$in": trial_ids}}
    proj = {"gs_uri": 1}
    data_query = "data?where=%s&projection=%s" % (
        json.dumps(condition),
        json.dumps(proj),
    )
    records = EVE_FETCHER.get(token=token, endpoint=data_query).json()["_items"]
    return [records["gs_uri"] for record in records]


def deactive_account(user_email: str, token: str) -> None:
    """
    Remove all data access from the supplied account.

    Arguments:
        user_email {str} -- Registered email of user to be deactivated.
        token {str} -- Eve access token.
    """
    # Get all trials where user is a collaborator.
    matched_trials = get_user_trials(user_email, token)

    # Update all of those trials to remove the individual.
    update = {"$pull": {"collaborators": user_email}}
    for trial in matched_trials:
        url = "trials/" + trial["_id"]
        headers = {"If-Match": trial["_etag"]}
        EVE_FETCHER.patch(endpoint=url, token=token, headers=headers, json=update)
        per_log = (
            "User: "
            + user_email
            + " removed as collaborator from trial: "
            + trial["_id"]
        )
        logging.info({"message": per_log, "category": "FAIR-CELERY-PERMISSIONS"})

    # Get list of records person is likely to be authorized on.
    gs_uri_list = get_user_records(matched_trials, token)

    # Remove their read and write access to records. Note they likely never had write.
    # But write is revoked anyway as a precaution.
    revoke_access(GOOGLE_BUCKET_NAME, gs_uri_list, [user_email])


def delete_user_account(user_email: str, token: str) -> None:
    """
    Delete a user account from the accounts collections.

    Arguments:
        user_email {str} -- User's registered email.
        token {str} -- API access token.
    """
    cond = {"email": user_email}
    projection = {"_id": 1}
    query = "accounts?where=%s&projection=%s" % (
        json.dumps(cond),
        json.dumps(projection),
    )
    user = EVE_FETCHER.get(endpoint=query, token=token).json()["_items"][0]
    url = "accounts/" + user["_id"]
    headers = {"If-Match": user["_etag"]}
    EVE_FETCHER.delete(endpoint=url, token=token, headers=headers)
    log = "Deleted user account: " + user_email
    logging.info({"message": log, "category": "FAIR-CELERY-ACCOUNTS"})


@APP.task(base=AuthorizedTask)
def test_eve_rate_limit(num_requests: int) -> bool:
    """
    Task to test if eve's rate-limit functionality is working.

    Arguments:
        num_requests {int} -- Number of requests to ping eve with

    Returns:
        bool -- True if application is rate limited, false if it fails for other reasons or
            fails to be limited.
    """
    for i in range(num_requests):
        try:
            EVE_FETCHER.get(
                token=test_eve_rate_limit.token["access_token"], endpoint="test"
            )
        except RuntimeError:
            if i > 0:
                print("rate-limit worked!")
                return True
            print("There seems to be a problem with your URL.")
            return False
        return False


@APP.task(base=AuthorizedTask)
def check_last_login() -> None:
    """
    Function that scans the user collection for inactive accounts and deletes any if found.
    """
    # Get list of accounts and their last logins.
    projection = {"last_access": 1, "email": 1}
    query = "accounts?projection=%s" % (json.dumps(projection))
    user_results = EVE_FETCHER.get(
        token=check_last_login.token["access_token"], endpoint=query
    ).json()["_items"]

    # Define relevant time periods and get current time.
    year = timedelta(days=365)
    month = timedelta(days=90)
    current_t = datetime.now(timezone.utc)

    # Deactive any accounts inactive for a month, delete any inactive for a year.
    for user in user_results:
        print(user)
        last_l = parse(user["last_access"])
        if current_t - last_l > month:
            deactive_account(user, check_last_login.token["access_token"])
        elif current_t - last_l > year:
            delete_user_account(user, check_last_login.token["access_token"])


def fetch_last_log_id() -> str:
    """
    Function that gets the ID of the last log comitted to the bucket.

    Returns:
        str -- ID of the log.
    """
    gs_args = ["gsutil", "cp", "gs://cidc-logstore/auth0/lastid.json", "./lastid.json"]
    subprocess.run(gs_args)
    log_json = None
    with open("lastid.json", "r") as last_id:
        log_json = json.load(last_id)
    remove("lastid.json")
    return log_json["_id"]


def update_last_id(last_log) -> None:
    """
    Updates the lastid file in google buckets to the new ID.

    Arguments:
        last_log {dict} -- Auth0 log entry
    """
    with open("lastid.json", "w") as log:
        json.dump(last_log, log)

    gs_args = ["gsutil", "cp", "lastid.json", AUTH0_DOMAIN + "/" + LOGSTORE + "/auth0"]
    subprocess.run(gs_args)


@APP.task(base=AuthorizedTask)
def poll_auth0_logs() -> None:
    """
    Function that polls the auth0 management API for new logs
    """
    # Get last log
    last_log_id = fetch_last_log_id()

    # Get new logs
    logs_endpoint = "%slogs?from=%s&sort=date%%3A1" % (MANAGEMENT_API, last_log_id)
    headers = {
        "Authorization": "Bearer {}".format(poll_auth0_logs.api_token["access_token"])
    }
    results = requests.get(logs_endpoint, headers=headers)
    gs_path = "gs://%s/auth0" % LOGSTORE

    if results.status_code != 200:
        log = "Failed to fetch auth0 logs, Reason: %s Status Code: %s" % (
            results.reason,
            results.status_code,
        )
        logging.warning({"message": log, "category": "WARNING-CELERY-LOGGING"})

    logs = results.json()

    # Update last log ID.
    last_log = logs[-1]
    update_last_id(last_log)

    for log_entry in logs:
        # Create temporary file
        temp_file_name = log_entry["date"]
        with open(temp_file_name, "w") as log_file:
            json.dump(log_entry, log_file)

        # Copy file to bucket
        subprocess.run(["gsutil", "cp", temp_file_name, gs_path])

    log = "Logging operation successfull, logs written to: " + gs_path
    logging.info({"message": log, "category": "FAIR-CELERY-LOGGING"})


def get_collabs(trial_id: str, token: str) -> dict:
    """
    Gets a list of collaborators given a trial ID

    Arguments:
        trial_id {str} -- ID of trial.
        token {str} -- Access token.

    Returns:
        dict -- Mongo response.
    """
    trial = {"_id": trial_id}
    projection = {"collaborators": 1}
    query = "trials?where=%s&projection=%s" % (
        json.dumps(trial),
        json.dumps(projection),
    )
    return EVE_FETCHER.get(token=token, endpoint=query)


def change_user_role(user_id: str, token: str, new_role: str, authorizer: str) -> None:
    """
    Change the role of a given user.

    Arguments:
        user_id {str} -- ID of the user's account.
        token {str} -- API access token.
        new_role {str} -- User's new role.
        authorizer {str} -- ID of authorizing admin.
    """
    url = "accounts/" + user_id
    user_doc = EVE_FETCHER.get(endpoint=url, token=token)
    headers = {"If-Match": user_doc["_etag"]}
    EVE_FETCHER.patch(
        endpoint=url, token=token, headers=headers, json={"role": new_role}
    )
    log = "Role change for user: %s from %s to %s authorized by: %s" % (
        user_doc["email"],
        user_doc["role"],
        new_role,
        authorizer,
    )

    logging.info({"message": log, "category": "FAIR-CELERY-ACCOUNTS"})


def manage_bucket_acl(bucket_name: str, gs_path: str, collaborators: List[str]) -> None:
    """
    Manages bucket authorization for accounts.

    Arguments:
        bucket_name {str} -- Name of the google bucket.
        gs_path {str} -- Path to object.
        collaborators {[str]} -- List of email addresses.
    """
    if not collaborators:
        logging.warning(
            {
                "message": "Manage bucket acl called with empty collaborators list",
                "category": "WARNING-CELERY-PERMISSIONS",
            }
        )
        return

    bucket = storage.Client().bucket(bucket_name)
    pathname = "gs://" + bucket_name
    blob_name = gs_path.replace(pathname, "")[1:]
    blob = bucket.blob(blob_name)

    # Filter out entries without identifiers
    identified = list(filter(lambda x: "identifier" in x, blob.acl))
    # If a person is already authorized, don't add them again.
    existing = [entry["identifier"] for entry in identified]
    # Check for discrepancies in the auth lists.
    to_deactivate = list(filter(lambda x: x not in collaborators, existing))
    to_add = list(filter(lambda x: x not in existing, collaborators))

    for person in to_add:
        log = "Gave read access to %s for object: %s" % (person, gs_path)
        logging.info({"message": log, "category": "FAIR-CELERY-PERMISSIONS"})
        blob.acl.user(person).grant_read()

    for person in to_deactivate:
        log = "Revoking accecss for %s for object: %s" % (person, gs_path)
        logging.warning({"message": log, "category": "FAIR-CELERY-PERMISSIONS"})
        blob.acl.user(person).revoke_read()
        blob.acl.user(person).revoke_write()

    blob.acl.save()


@APP.task(base=AuthorizedTask)
def update_trial_blob_acl(trial_id: str, new_acl: List[str]) -> None:
    """
    Updates all access control lists for all blobs associated with a given trial.

    Arguments:
        trial_id {str} -- ID of the trial in question.
        new_acl {List[str]} -- Up to date list of collaborators on the project.
    """
    # Get all data from the project.
    condition = {"trial": trial_id}
    projection = {"gs_uri": 1}
    query = "data?where=%sprojection=%s" % (
        json.dumps(condition),
        json.dumps(projection),
    )
    trial_data = EVE_FETCHER.get(
        endpoint=query, token=update_trial_blob_acl.token["access_token"]
    ).json()["_items"]
    gs_paths = [x["gs_uri"] for x in trial_data]

    # Send the new access control list to the manager function, new users get added
    # removed users get access revoked.
    for path in gs_paths:
        manage_bucket_acl(GOOGLE_BUCKET_NAME, path, new_acl)


@APP.task(base=AuthorizedTask)
def grant_bucket_upload(bucket_name, user_emails):
    bucket = storage.Client().bucket(bucket_name)
    for email in user_emails:
        bucket.acl.user(email).grant_write()


def revoke_access(bucket_name: str, gs_paths: List[str], emails: List[str]) -> None:
    """
    Revokes access to a given object for a list of people.

    Arguments:
        bucket_name {str} -- Name of the google bucket.
        gs_paths {[str]} -- List of affected record uris.
        emails {[str]} -- List of email addresses.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for path in gs_paths:
        pathname = "gs://" + bucket_name
        blob_name = path.replace(pathname, "")[1:]
        blob = bucket.blob(blob_name)

        blob.acl.reload()
        for person in emails:
            log = "Revoked read/write access from " + person + " for object: " + path
            logging.info({"message": log, "category": "FAIR-CELERY-PERMISSIONS"})
            blob.acl.user(person).revoke_read()
            blob.acl.user(person).revoke_write()

        blob.acl.save()
