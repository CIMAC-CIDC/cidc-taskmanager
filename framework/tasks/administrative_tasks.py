#!/usr/bin/env python
"""
These tasks are responsible for performing administrative and user management tasks.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

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
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.variables import (
    AUTH0_DOMAIN,
    EVE_URL,
    GOOGLE_BUCKET_NAME,
    GOOGLE_UPLOAD_BUCKET,
    LOGSTORE,
    MANAGEMENT_API
)

EVE_FETCHER = SmartFetch(EVE_URL)


def get_user_trials(user_email: str, token: str) -> List[dict]:
    """
    Gets the ids of all trials given user is a collaborator on.

    Arguments:
        user_email {str} -- User's email.
        token {str} -- JWT for API access.

    Returns:
        List[dict] -- List of trials user is a collaborator on.
    """
    collabs = {"collaborators": user_email}
    projection = {"_id": 1}
    query = "trials?where=%s&projection=%s" % (
        json.dumps(collabs),
        json.dumps(projection),
    )
    return EVE_FETCHER.get(token=token, endpoint=query).json()["_items"]


def get_user_records(permissions: List[dict], token: str) -> List[str]:
    """
    Gets all of the data objects the user might have permission to see.

    Arguments:
        permissions {List[dict]} -- List of permissions for a user.
        token {str} -- Access token.

    Returns:
        List[str] -- List of google URIs.
    """
    trial_access = []
    assay_access = []
    conditions = {"$or": []}
    for perm in permissions:
        if perm["role"] in ["trial_r", "trial_w"]:
            trial_access.append(perm["trial"])
            conditions["$or"].append({"trial": perm["trial"]})
        if perm["role"] in ["assay_r", "assay_w"]:
            assay_access.append({"assay": perm["assay"]})
            conditions["$or"].append(perm["assay"])

    for perm in permissions:
        if perm["trial"] not in trial_access and perm["assay"] not in assay_access:
            conditions["or"].append({"trial": perm["trial"], "assay": perm["assay"]})

    query = "data?where=%s" % json.dumps(conditions)
    records = EVE_FETCHER.get(token=token, endpoint=query).json()["items"]
    return [records["gs_uri"] for record in records]


def clear_permissions(user_id: str, token: str) -> None:
    """
    Clears the permissions list for a given user.

    Arguments:
        user_id {str} -- ID of user to be cleared.
        token {str} -- Access token.

    Returns:
        None -- [description]
    """
    query = "accounts/%s" % user_id
    user_etag = EVE_FETCHER.get(endpoint=query, token=token)["_etag"]
    update = {"permissions": []}
    try:
        EVE_FETCHER.patch(
            endpoint="accounts", _etag=user_etag, token=token, json=update
        )
    except RuntimeError:
        log = "Error attempting to clear permissions for user %s" % user_id
        logging.error({"message": log, "category": "ERROR-CELERY-USER-FAIR"})


def deactivate_account(user: dict, token: str) -> None:
    """
    Remove all data access from the supplied account.

    Arguments:
        user {dict} -- user record containing id, email, last access.
        token {str} -- Eve access token.
    """
    # Get all trials where user is a collaborator.
    matched_trials = get_user_trials(user["email"], token)

    # Update all of those trials to remove the individual.
    update = {"$pull": {"collaborators": user["email"]}}
    trial_names = []

    for trial in matched_trials:
        try:
            trial_names.append(trial["trial_name"])
            EVE_FETCHER.patch(
                endpoint="trials", item_id=trial["_id"], token=token, json=update
            )
        except RuntimeError:
            per_log = "Error trying to delete %s from collaborators for trial %s" % (
                user["email"],
                trial["_id"],
            )
            logging.error({"message": per_log, "category": "ERROR-CELERY-ACCOUNTS"})

    message = "User: %s removed as a collaborator from the following trials: %s" % (
        user["email"],
        ", ".join(trial_names),
    )
    logging.info({"message": message, "category": "FAIR-CELERY-PERMISSIONS"})

    # Get list of records person is likely to be authorized on.
    gs_uri_list = get_user_records(user["permissions"], token)
    revoke_access(GOOGLE_BUCKET_NAME, gs_uri_list, [user["email"]])
    clear_permissions(user["_id"], token)
    change_upload_permission(GOOGLE_UPLOAD_BUCKET, [user], False)


def delete_user_account(user: dict, token: str) -> None:
    """
    Delete a user account from the accounts collections.

    Arguments:
        user {dict} -- User object.
        token {str} -- API access token.
    """
    url = "accounts/%s" % user["_id"]
    user_record = EVE_FETCHER.get(endpoint=url, token=token).json()
    headers = {"If-Match": user_record["_etag"]}
    EVE_FETCHER.delete(endpoint=url, token=token, headers=headers)
    log = "Deleted user account: %s" % user["email"]
    logging.info({"message": log, "category": "FAIR-CELERY-ACCOUNTS"})
    gs_uri_list = get_user_records(user["permissions"], token)
    revoke_access(GOOGLE_BUCKET_NAME, gs_uri_list, [user["email"]])
    change_upload_permission(GOOGLE_UPLOAD_BUCKET, [user], False)


@APP.task(base=AuthorizedTask)
def call_deactivate_account(user: dict, delete: bool) -> None:
    """
    Wrapper function for admins to deactivate or delete an account.

    Arguments:
        user {dict} -- Account record of the user.
        delete {bool} -- True if the user is to be deleted, false if they are only disabled.
    """
    if delete:
        delete_user_account(user, call_deactivate_account.token["access_token"])
    else:
        deactivate_account(user, call_deactivate_account.token["access_token"])


@APP.task(base=AuthorizedTask)
def test_eve_rate_limit(num_requests: int) -> bool:
    """
    Task to test if eve's rate-limit functionality is working.

    Arguments:
        num_requests {int} -- Number of requests to ping eve with.

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
    user_results = EVE_FETCHER.get(
        token=check_last_login.token["access_token"], endpoint="accounts"
    ).json()["_items"]

    # Define relevant time periods and get current time.
    year = timedelta(days=365)
    month = timedelta(days=90)
    current_t = datetime.now(timezone.utc)

    # Deactive any accounts inactive for a month, delete any inactive for a year.
    for user in user_results:
        last_l = parse(user["last_access"])
        if current_t - last_l > month:
            deactivate_account(user, check_last_login.token["access_token"])
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
        last_log {dict} -- Auth0 log entry.
    """
    with open("lastid.json", "w") as log:
        json.dump(last_log, log)
    gs_args = ["gsutil", "cp", "lastid.json", AUTH0_DOMAIN + "/" + LOGSTORE + "/auth0"]
    subprocess.run(gs_args)


@APP.task(base=AuthorizedTask)
def poll_auth0_logs() -> None:
    """
    Function that polls the auth0 management API for new logs.
    """
    last_log_id = fetch_last_log_id()
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


def get_authorized_users(record_info: dict, token: str) -> List[str]:
    """For a given record, return a list of users who can see it.

    Arguments:
        record_info {dict} -- Dictionary with trial and assay id.
        token {str} -- access token.

    Returns:
        List[str] -- List of user emails.
    """
    assay = record_info["assay"]
    trial = record_info["trial"]
    query = {
        "$or": [
            {"permissions": {"trial": trial, "role": "trial_r"}},
            {"permissions": {"trial": trial, "role": "trial_w"}},
            {"permissions": {"assay": assay, "role": "assay_r"}},
            {"permissions": {"assay": assay, "role": "assay_w"}},
            {"permissions": {"trial": trial, "assay": assay, "role": "read"}},
            {"permissions": {"trial": trial, "assay": assay, "role": "write"}},
        ]
    }
    authorized_users = EVE_FETCHER.get(
        "accounts?where=%s" % json.dumps(query), token=token
    ).json()["_items"]
    return [user["email"] for user in authorized_users]


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
    EVE_FETCHER.patch(
        endpoint=url, token=token, _etag=user_doc["_etag"], json={"role": new_role}
    )
    log = "Role change for user: %s from %s to %s authorized by: %s" % (
        user_doc["email"],
        user_doc["role"],
        new_role,
        authorizer,
    )
    logging.info({"message": log, "category": "FAIR-CELERY-ACCOUNTS"})


def manage_bucket_acl(bucket_name: str, gs_path: str, authorized_users: List[str]) -> None:
    """
    Manages bucket authorization for accounts.

    Arguments:
        bucket_name {str} -- Name of the google bucket.
        gs_path {str} -- Path to object.
        authorized_users {[str]} -- List of email addresses.
    """
    if not authorized_users:
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
    to_deactivate = list(filter(lambda x: x not in authorized_users, existing))
    to_add = list(filter(lambda x: x not in existing, authorized_users))

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


@APP.task
def change_upload_permission(
    bucket_name: str, user_emails: List[str], grant_or_revoke: bool
) -> None:
    """
    Changes the user's permissions to upload to a bucket.

    Arguments:
        bucket_name {str} -- Name of the bucket.
        user_emails {List[str]} -- Users to change the access of.
        grant_or_revoke {bool} -- True if the users are to be granted access, else false.

    Returns:
        None -- [description]
    """
    bucket = storage.Client().bucket(bucket_name)
    action = "granted" if grant_or_revoke else "revoked"
    for email in user_emails:
        if grant_or_revoke:
            bucket.acl.user(email).grant_write()
        else:
            bucket.acl.user(email).revoke_read()
            bucket.acl.user(email).revoke_write()
    log = "Access %s to bucket %s for users: %s" % (
        action,
        bucket_name,
        ", ".join(user_emails),
    )
    logging.info({"message": log, "category": "FAIR-CELERY-PERMISSIONS"})


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
            blob.acl.user(person).revoke_read()
            blob.acl.user(person).revoke_write()

        blob.acl.save()

    message = "Access to objects: %s. Revoked for users: %s" % (
        ", ".join(gs_paths),
        ", ".join(emails),
    )
    logging.info({"message": message, "category": "FAIR-CELERY-PERMISSIONS"})
