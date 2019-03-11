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
import re
import subprocess
from typing import List, NamedTuple, Tuple

from cidc_utils.requests import SmartFetch
from google.cloud import storage
from snakemake import snakemake

from framework.celery.celery import APP
from framework.tasks.administrative_tasks import get_authorized_users, manage_bucket_acl
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.storage_tasks import run_subprocess_with_logs
from framework.tasks.parallelize_tasks import execute_in_parallel
from framework.tasks.variables import EVE_URL, GOOGLE_BUCKET_NAME
from framework.tasks.analysis_tasks import set_record_processed, check_processed

EVE = SmartFetch(EVE_URL)
FILE_EXTENSION_DICT = {"fa": "FASTQ", "fa.gz": "FASTQ", "fq.gz": "FASTQ"}
SUFFIX_REGEX = re.compile(r"((\.[^\.]*){1,2}$)")


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
    singularity: bool


DEFAULT_SETTINGS = SnakeJobSettings(
    6,
    8000,
    "default",
    [KubeToleration("NoSchedule", "snakemake", "Equal", "issnake")],
    True,
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
        assay_response = EVE.get(
            token=token, endpoint=assay_query_string, code=200
        ).json()["_items"]
        sought_mappings = [
            item
            for sublist in [x["non_static_inputs"] for x in assay_response]
            for item in sublist
        ]
        query_string = "data/query?aggregate=%s" % (
            json.dumps({"$inputs": sought_mappings})
        )
        record_response = EVE.get(token=token, endpoint=query_string, code=200)
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
        try:
            str(query_string)
            error_msg = "Failed to fetch record from data collection: %s" % str(rte)
        except NameError:
            error_msg = "Failed to fetch record from assays collection: %s" % str(rte)
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
        ["git", "clone", "--single-branch", "--branch", "jason", git_url, folder_name],
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
        default_remote_prefix=GOOGLE_BUCKET_NAME,
        default_remote_provider="GS",
        use_singularity=kube_settings.singularity,
    )


def create_input_json(
    records: List[dict], run_id: str, cimac_sample_id: str
) -> List[dict]:
    """
    Creates the inputs.json for a snakemake run.

    Arguments:
        records {List[dict]} -- List of input records to the run.
        run_id {str} -- _id representing run_id, uses value of analysis record _id.
        cimac_sample_id {str} -- Sample_id of run.

    Returns:
        List[dict] -- List of the input files used in the run.
    """
    files_used = []
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
            "gs://%s/" % (GOOGLE_BUCKET_NAME), ""
        )
        files_used.append(
            {
                "file_name": record["file_name"],
                "gs_uri": record["gs_uri"],
                "data_id": record["_id"],
            }
        )

    # GSUTIL copy references....
    for reference, location in inputs["reference_files"].items():
        new_path = run_id + "/" + location
        gsutil_args = [
            "gsutil",
            "cp",
            run_id + "/" + location,
            "gs://%s/%s" % (GOOGLE_BUCKET_NAME, new_path),
        ]
        run_subprocess_with_logs(gsutil_args, "Uploading references")
        inputs["reference_files"][reference] = new_path
    # Delete old file:
    os.remove(inputs_file)

    # Write new file.
    with open(inputs_file, "w") as outfile:
        json.dump(inputs, outfile)

    return files_used


def register_analysis(valid_run: Tuple[dict, dict], token: str) -> dict:
    """
    Register an analysis job.

    Arguments:
        valid_run {Tuple[dict, dict]} -- Records + assay.

    Returns:
        dict -- Dictionary with _id and _etag.
    """
    payload = valid_run[0]["_id"]
    payload["workflow_location"] = valid_run[1]["workflow_location"]
    payload["start_date"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload["status"] = "In Progress"
    results = EVE.post(endpoint="analysis", token=token, code=201, json=payload).json()
    return {"_id": results["_id"], "_etag": results["_etag"]}


def upload_snakelogs(analysis_id: str) -> List[str]:
    """
    Upload snakemake's logs to google bucket.

    Arguments:
        analysis_id {str} -- Id of the analysis run.

    Returns:
        List[str] -- List of gs uris for snakelogs.
    """
    snakelogs: List[str] = os.listdir("%s/.snakemake/log/" % analysis_id)
    snakelogs_fullpath = [
        "%s/.snakemake/log/%s" % (analysis_id, logname) for logname in snakelogs
    ]
    filenames = str.join("\n", snakelogs_fullpath)
    fileprint = subprocess.Popen(("printf", filenames), stdout=subprocess.PIPE)
    gsutil_args = [
        "gsutil",
        "-m",
        "cp",
        "-I",
        "gs://%s/runs/%s/logs" % (GOOGLE_BUCKET_NAME, analysis_id),
    ]
    subprocess.check_output(gsutil_args, stdin=fileprint.stdout)
    fileprint.wait()
    return [
        "gs://%s/runs/%s/logs/%s" % (GOOGLE_BUCKET_NAME, analysis_id, log)
        for log in snakelogs
    ]


def analyze_jobs(dag) -> Tuple[List[dict], List[str]]:
    """
    Analyze a list of job objects and return information.

    Arguments:
        dag {} -- Snakemake dag object.

    Returns:
        Tuple[List[dict], List[str]] -- Job entries, output urls.
    """
    job_list: List[dict] = []
    output_list: List[str] = []
    for job in dag.jobs:
        job_entry = {
            "job_name": job.name,
            "log_locations": job.log,
            "completed": bool(job in dag.finished_jobs),
            "inputs": [],
            "outputs": [],
        }
        try:
            job_entry["inputs"] = job.input
            outputs = job.output
            output_list = output_list + outputs
            job_entry["outputs"] = outputs

        except AttributeError:
            pass
        job_list.append(job_entry)
    return job_list, output_list


def get_data_format(name: str) -> str:
    """
    Takes a file's name and returns a data format.

    Arguments:
        name {str} -- File name.

    Returns:
        str -- Data format.
    """
    try:
        extension = re.search(SUFFIX_REGEX, name).group(0)
        data_format = FILE_EXTENSION_DICT.get(extension, extension[1::].upper())
    except AttributeError:
        data_format = "UNKNOWN"
    return data_format


def upload_results(valid_run: dict, outputs: List[str], token: str) -> List[dict]:
    """
    Upload the output files from a run.

    Arguments:
        valid_run {dict} -- Run information.
        outputs {List[str]} -- List of GS-uris of output files.
        token {str} -- JWT

    Returns:
        List[dict] -- List of inserted records.
    """
    payload = []
    bucket = storage.Client().get_bucket(GOOGLE_BUCKET_NAME)
    aggregation_res = valid_run[0]["_id"]
    for output in outputs:
        prefix = output.replace(GOOGLE_BUCKET_NAME + "/", "")
        logging.info({"message": prefix, "category": "PREFIX-EBUG"})
        output_file = [item for item in bucket.list_blobs(prefix=prefix)][0]
        payload.append(
            {
                "data_format": get_data_format(output),
                "date_created": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
                "file_name": output_file.name.split("/")[-1],
                "uuid_alias": "unaliased",
                "file_size": output_file.size,
                "sample_ids": aggregation_res["sample_ids"],
                "number_of_samples": len(aggregation_res["sample_ids"]),
                "trial": aggregation_res["trial"],
                "trial_name": aggregation_res["trial_name"],
                "assay": aggregation_res["assay"],
                "gs_uri": "gs://%s/%s" % (GOOGLE_BUCKET_NAME, output_file.name),
                "mapping": "",
                "experimental_strategy": aggregation_res["experimental_strategy"],
                "processed": False,
                "visibility": True,
                "children": [],
            }
        )
        authorized_users = get_authorized_users(
            {"assay": aggregation_res["assay"], "trial": aggregation_res["trial"]},
            token,
        )
        manage_bucket_acl(
            GOOGLE_BUCKET_NAME, payload[-1]["gs_uri"], authorized_users
        )
    try:
        inserts = EVE.post(
            endpoint="data_edit", code=201, token=token, json=payload
        ).json()["_items"]
        logging.info(
            {
                "message": "Records created for run outputs",
                "category": "FAIR-CELERY-SNAKEMAKE",
            }
        )
        return [
            {"data_id": x["_id"], "gs_uri": y["gs_uri"], "file_name": y["file_name"]}
            for x, y in zip(inserts, payload)
        ]
    except RuntimeError as rte:
        message = "Upload of run outputs failed: %s" % str(rte)
        logging.error({"message": message, "category": "ERROR-CELERY-UPLOAD"})
        return None


def update_analysis(
    valid_run: Tuple[dict, dict], token: str, analysis: dict, dag, problem=None
) -> bool:
    """
    Update analysis record with results when the analysis has completed.

    Arguments:
        valid_run {Tuple[dict, dict]} -- Information about the run.
        token {str} -- JWT
        analysis {dict} -- Analysis id, _etag, input files.
        dag {snakemake.dag.DAG} -- DAG objet from the finished run.
        problem {str} -- Error message, string "Error" or None.

    Returns:
        bool -- True if update completes without error, else false.
    """
    payload = valid_run[0]["_id"]
    payload["workflow_location"] = valid_run[1]["workflow_location"]
    jobs, outputs = analyze_jobs(dag)
    payload["jobs"] = jobs
    payload["snakemake_logs"] = upload_snakelogs(analysis["_id"])
    payload["files_used"] = analysis["files_used"]
    payload["end_date"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # If job succeeds, upload files.
    if outputs and not problem:
        payload["files_generated"] = upload_results(valid_run, outputs, token)

    if problem:
        payload["status"] = "Failed"
        if problem == "Error":
            payload["error_message"] = (
                "One of the jobs experienced an error,"
                + "please consult the logs for more information"
            )
        else:
            payload["error_message"] = "Snakefile Error: %s" % str(problem)
    else:
        payload["status"] = "Completed"

    # some upload function.
    try:
        EVE.patch(
            endpoint="analysis",
            token=token,
            _etag=analysis["_etag"],
            item_id=analysis["_id"],
            json=payload,
            code=200,
        )
    except RuntimeError as rte:
        str_error = "Analysis update failed: %s" % str(rte)
        logging.error({"message": str_error, "category": "ERROR-CELERY-PATCH"})
        return False
    return True


@APP.task(base=AuthorizedTask)
def execute_workflow(valid_run: Tuple[dict, dict]):
    """
    Create inputs, run snakemake, report results.

    Arguments:
        valid_run {Tuple[dict, dict]} -- Tuple of (aggregation result, assay info)
    """
    # Check that all records are unprocessed.
    token = execute_workflow.token["access_token"]
    records, all_free = check_processed(valid_run[0]["records"], token)
    analysis_response = register_analysis(valid_run, token)
    if not all_free:
        logging.error(
            {
                "message": "Some input files are processed, check celery logic!",
                "category": "ERROR-CELERY-SNAKEMAKE",
            }
        )
        return False
    logging.info(
        {"message": "Setting files to processed", "category": "INFO-CELERY-SNAKEMAKE"}
    )
    set_record_processed(records, True, token)

    # reference shortcuts
    aggregation_res = valid_run[0]["_id"]
    cimac_sample_id: str = aggregation_res["sample_ids"][0]
    run_id: str = str(analysis_response["_id"])

    snakemake_file = clone_snakemake(valid_run[1]["workflow_location"], run_id)
    analysis_response["files_used"] = create_input_json(
        valid_run[0]["records"], run_id, cimac_sample_id
    )

    # Run Snakefile.
    workflow_dag, problem = run_snakefile(snakemake_file, workdir=run_id)

    # If run fails, log it, reset records, return False.
    if problem:
        logging.error(
            {"message": "Snakemake run failed!", "category": "ERROR-CELERY-SNAKEMAKE"}
        )
        records, all_free = check_processed(valid_run[0]["records"], token)
        set_record_processed(records, False, token)
        error_str = str(problem)
        logging.error({"message": error_str, "category": "ERROR-CELERY-SNAKEMAKE"})
        update_analysis(
            valid_run, token, analysis_response, workflow_dag, problem=problem
        )
        return False

    # Update analysis entry w/ success
    update_analysis(valid_run, token, analysis_response, workflow_dag)
    return True


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
        try:
            execute_in_parallel(run_tasks, 1000, 10)
            logging.info(
                {
                    "message": "Snakemake run successful",
                    "category": "INFO-CELERY-SNAKEMAKE",
                }
            )
        except RuntimeError as rte:
            str_log = "Snakemake run failed: %s" % str(rte)
            logging.info({"message": str_log, "category": "ERROR-CELERY-SNAKEMAKE"})
