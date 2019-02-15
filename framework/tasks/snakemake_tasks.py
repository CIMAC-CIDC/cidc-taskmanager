#!/usr/bin/env python
"""
Module for snakemake related functions
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import datetime
import json
import logging
import os
from typing import List, NamedTuple, Tuple
from uuid import uuid4

from cidc_utils.requests import SmartFetch
from google.cloud import storage
from snakemake import snakemake

from framework.celery.celery import APP
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.cromwell_tasks import run_subprocess_with_logs
from framework.tasks.parallelize_tasks import execute_in_parallel
from framework.tasks.variables import EVE_URL
from framework.tasks.analysis_tasks import set_record_processed, check_processed

EVE = SmartFetch(EVE_URL)


class KubeToleration(NamedTuple):
    """
    Representation of a Kubernetes toleration block.

    Arguments:
        NamedTuple {[type]} -- [description]
    """

    effect: str
    key: str
    operator: str
    value: str


class SnakeJobSettings(NamedTuple):
    """
    Kubernetes settings for snakemake jobs.

    Arguments:
        NamedTuple {[type]} -- [description]
    """

    cpu: int
    memory: int
    namespace: str
    tolerations: List[KubeToleration]


DEFAULT_SETTINGS = SnakeJobSettings(
    6, 8000, "default", [KubeToleration("NoSchedule", "snakemake", "Equal", "issnake")]
)


def check_for_runs(token: str) -> Tuple[List[dict], List[dict]]:
    """
    Checks to see if any runs can start

    Arguments:
        token {str} -- JWT for API.

    Returns:
        Tuple[List[dict], List[dict]] -- Tuple, where each item is a data/assay pairing
        representing a run that can be started.
    """

    try:
        # Only return assays that have a workflow associated with them
        assay_query = {"workflow_location": {"$ne": "null"}}
        assay_query_string = "assays?where=%s" % json.dumps(assay_query)

        # Contains a list of all the running assays and their inputs
        assay_response = EVE.get(token=token, endpoint=assay_query_string).json()["_items"]
        sought_mappings = [
            item
            for sublist in [x["non_static_inputs"] for x in assay_response]
            for item in sublist
        ]
        query_string = "data/query?aggregate=%s" % (
            json.dumps({"$inputs": sought_mappings})
        )
        record_response = EVE.get(token=token, endpoint=query_string)
        # Create an assay id keyed dictionary to simplify searching.
        assay_dict = {
            assay["_id"]: {
                "non_static_inputs": assay["non_static_inputs"],
                "assay_name": assay["assay_name"],
                "workflow_location": assay["workflow_location"],
            }
            for assay in assay_response
        }
        return record_response, assay_dict
    except RuntimeError as rte:
        error_msg = "Failed to fetch record: %s" % str(rte)
        logging.error({"message": error_msg, "category": "ERROR-CELERY-QUERY"})
        return None


def find_valid_runs(record_response: dict, assay_dict: dict) -> List[Tuple[dict, dict]]:
    """
    Checks to see if any runs are possible.

    Arguments:
        record_response {dict} -- Return from the aggregation query of the /data endpoint.
        assay_dict {dict} -- Dictionary keyed on assay_id containing assay info.

    Returns:
        List[dict] -- Tuple of a file group, and the assay which it should run.
    """
    valid_runs = []
    for grouping in record_response:
        assay_id = grouping["_id"]["assay"]
        try:
            mappings_present = [record["mapping"] for record in grouping["records"]]
            if set(mappings_present) == set(assay_dict[assay_id]["non_static_inputs"]):
                # If mappings are 1:1, a run may start.
                valid_runs.append((grouping, assay_dict[assay_id]))
        except KeyError:
            logging.error(
                {
                    "message": "A data file with an invalid assay id was found",
                    "category": "ERROR-CELERY-SNAKEMAKE",
                }
            )
            return None
    return valid_runs


def clone_snakemake(git_url: str, folder_name: str) -> str:
    """
    Clones snakemake location and returns the path of the Snakefile.

    Arguments:
        git_url {str} -- GitHub URL for snakemake workflow.
        folder_name {str} -- Name of the folder to create.

    Returns:
        str -- Snakefile path.
    """
    run_subprocess_with_logs(
        [
            "git",
            "clone",
            "--single-branch",
            "--branch",
            "single_sample",
            git_url,
            folder_name,
        ],
        "cloning snakemake",
    )
    return folder_name + "/Snakefile"


@APP.task
def run_snakefile(
    snakefile_path: str = "./Snakefile",
    workdir: str = "./",
    kube_settings: SnakeJobSettings = DEFAULT_SETTINGS,
) -> bool:
    """
    Run the snakemake job with custom settings.

    Arguments:
        snakefile_path {str} -- Path to snakefile.

    Keyword Arguments:
        kube_settings {SnakeJobSettings} -- Settings that allow resource definitions
        + tolerations (default: {DEFAULT_SETTINGS})

    Returns:
        bool -- True if the run worked, else false.
    """
    # run the snakemake job
    return snakemake(
        snakefile_path,
        workdir=workdir,
        kubernetes=kube_settings.namespace,
        kubernetes_resource_requests={
            "cpu": kube_settings.cpu,
            "memory": kube_settings.memory,
        },
        kubernetes_tolerations=[
            tolerance._asdict() for tolerance in kube_settings.tolerations
        ],
        default_remote_prefix="lloyd-test-pipeline",
        default_remote_provider="GS",
    )


def create_input_json(records: List[dict], run_id: str, cimac_sample_id: str):
    """
    Creates the inputs.json for a snakemake run.

    Arguments:
        records {List[dict]} -- List of input records to the run.
        run_id {str} -- str(UUID4) representing run_id
        cimac_sample_id {str} -- sample_id of run.
    """
    # Read inputs.json
    inputs_file: str = run_id + "/inputs.json"
    with open(inputs_file, "r") as jso:
        inputs: dict = json.load(jso)

    # Fix up the inputs.json
    inputs["run_id"] = run_id
    inputs["meta"]["CIMAC_SAMPLE_ID"] = cimac_sample_id
    # Blank this block so it can be redefined
    inputs["sample_files"] = {}
    for record in records:
        inputs["sample_files"][record["mapping"]] = record["gs_uri"].replace(
            "gs://lloyd-test-pipeline/", ""
        )

    # GSUTIL copy references....
    for reference, location in inputs["reference_files"].items():
        new_path = run_id + "/" + location
        gsutil_args = [
            "gsutil",
            "cp",
            run_id + "/" + location,
            "gs://lloyd-test-pipeline/" + new_path,
        ]
        run_subprocess_with_logs(gsutil_args, "Uploading references")
        inputs["reference_files"][reference] = new_path
    # Delete old file:
    os.remove(inputs_file)

    # Write new file.
    with open(inputs_file, "w") as outfile:
        json.dump(inputs, outfile)


def map_outputs(run_id: str, cimac_sample_id: str, aggregation_res: dict) -> List[dict]:
    """
    Finds the outputs from the output file and returns them in /data format.

    Arguments:
        run_id {str} -- Id of the run.
        cimac_sample_id {str} -- Sample ID being analyzed.
        aggregation_res {dict} -- Result from the aggregation query.

    Returns:
        List[dict] -- List of /data format records for upload.
    """
    output_base_url: str = "runs/%s/results" % run_id

    # Parse output map
    output_uri: str = run_id + "/output_schema.json"
    with open(output_uri, "r") as out:
        outputs: dict = json.load(out)
    bucket = storage.Client().get_bucket("lloyd-test-pipeline")

    payload = []
    for output_name, output_location in outputs.items():
        prefix = output_base_url + "/" + output_location
        try:
            output_file = [item for item in bucket.list_blobs(prefix=prefix)][0]
        except IndexError:
            log = "File %s could not be found" % prefix
            logging.error({"message": log, "category": "ERROR-CELERY-SNAKEMAKE"})
        payload.append(
            {
                "data_format": output_name,
                "date_created": datetime.datetime.now().isoformat(),
                "file_name": output_file.name.split("/")[-1],
                "file_size": output_file.size,
                "sample_ids": [cimac_sample_id],
                "number_of_samples": 1,
                "trial": aggregation_res["trial"],
                "trial_name": aggregation_res["trial_name"],
                "assay": aggregation_res["assay"],
                "gs_uri": output_file.path[3:],
                "mapping": output_name,
                "experimental_strategy": aggregation_res["experimental_strategy"],
                "processed": False,
                "visibility": True,
                "children": [],
            }
        )
    return payload


@APP.task(base=AuthorizedTask)
def execute_workflow(valid_run: Tuple[dict, dict]):
    """
    Create inputs, run snakemake, report results.

    Arguments:
        valid_run {Tuple[dict, dict]} -- Tuple of (aggregation result, assay info)
    """
    records, all_free = check_processed(valid_run[0]["records"])
    logging.info(
        {"message": "Setting files to processed", "category": "INFO-CELERY-SNAKEMAKE"}
    )
    set_record_processed(records, True)
    aggregation_res = valid_run[0]["_id"]
    cimac_sample_id: str = aggregation_res["sample_ids"][0]
    run_id: str = str(uuid4())
    snakemake_file = clone_snakemake(valid_run[1]["workflow_location"], run_id)
    create_input_json(valid_run[0]["records"], run_id, cimac_sample_id)
    result: bool = run_snakefile(snakemake_file, workdir=run_id)
    if not result:
        logging.error(
            {"message": "Snakemake run failed!", "category": "ERROR-CELERY-SNAKEMAKE"}
        )
    payload = map_outputs(run_id, cimac_sample_id, aggregation_res)
    try:
        upload_results_res = EVE.post(
            endpoint="data_edit",
            token=execute_workflow.token["access_token"],
            json=payload,
            code=201
        )
        if upload_results_res.status_code >= 200:
            logging.info(
                {
                    "message": "Upload of output files succesful",
                    "category": "INFO-CELERY-FAIR-SNAKEMAKE",
                }
            )
        return True
    except RuntimeError as rte:
        message = "Upload of snakemake results failed: %s" % str(rte)
        logging.error({"message": message, "category": "ERROR-CELERY-SNAKEMAKE"})
        return False


@APP.task(base=AuthorizedTask)
def manage_workflows():
    """
    Function called by the API when new data is uploaded.
    """
    # Search for files and mappings.
    record_response, assay_dict = check_for_runs(manage_workflows.token["access_token"])

    if not record_response or not assay_dict:
        logging.error(
            {
                "message": "Data aggregation query failed",
                "category": "ERROR-CELERY-API-SNAKEMAKE",
            }
        )

    # Compute that into valid runs.
    valid_runs = find_valid_runs(record_response.json()["_items"], assay_dict)
    # Create a list of tasks to execute
    run_tasks = [execute_workflow.s(run) for run in valid_runs]
    # Spin the tasks out.
    if run_tasks:
        logging.info(
            {"message": "Snakemake tasks starting", "category": "INFO-CELERY-SNAKEMAKE"}
        )
        results = execute_in_parallel(run_tasks, 1000, 10)
        if results:
            logging.info(
                {
                    "message": "Snakemake run succesful",
                    "category": "INFO-CELERY-SNAKEMAKE",
                }
            )
        else:
            logging.info(
                {
                    "message": "Snakemake run failed",
                    "category": "ERROR-CELERY-SNAKEMAKE",
                }
            )
