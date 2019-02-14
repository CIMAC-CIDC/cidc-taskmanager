#!/usr/bin/env python
"""
Utility functions related to gene symbol validation.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import logging
import json
from ftplib import FTP
import gzip
from io import BytesIO
from typing import List
from cidc_utils.requests import SmartFetch
from framework.celery.celery import APP
from framework.tasks.authorized_task import AuthorizedTask
from framework.tasks.variables import EVE_URL
from framework.tasks.parallelize_tasks import execute_in_parallel


HUGO_URL = "ftp.ncbi.nlm.nih.gov"
HUGO_DIRECTORY = "gene/DATA/GENE_INFO/Mammalia"
HUGO_FILE_NAME = "Homo_sapiens.gene_info.gz"
EVE = SmartFetch(EVE_URL)
IDENTIFIER_FIELDS = [
    {"key": 2, "is_array": False},  # Symbol
    {"key": 4, "is_array": True},  # Synonyms
    {"key": 10, "is_array": False},  # Symbol_from_nomenclature_authority
]


def mk_error(
    explanation: str,
    affected_paths: List[str],
    raw_or_parse: str = "PARSE",
    severity: str = "WARNING",
) -> dict:
    """
    Handy function for creating a validation error.

    Arguments:
        explanation {str} -- Long form explanation of the error.
        affected_paths {List[str]} -- List of the json paths in the mongo record affected by the
            error.

    Keyword Arguments:
        raw_or_parse {str} -- Whether the issue is with the file object or the content.
            (default: {"PARSE"})
        severity {str} -- How severe the error is. (default: {"WARNING"})

    Returns:
        dict -- Validation error.
    """
    return {
        "explanation": explanation,
        "affected_paths": affected_paths,
        "raw_or_parse": raw_or_parse,
        "severity": severity,
    }


@APP.task(base=AuthorizedTask)
def chunked_upload(symbols: List[dict]):
    """
    Posts to the gene_symbols endpoint, declared here as a task for execution purposes.

    Arguments:
        symbols {List[dict]} -- List of gene symbols.
    """
    EVE.post(
        endpoint="gene_symbols",
        token=chunked_upload.token["access_token"],
        code=201,
        json=symbols,
    )


@APP.task(base=AuthorizedTask)
def refresh_hugo_defs():
    """
    Periodic task meant to keep the hugo definitions up to date.
    """
    hugo_string = get_gz_ftp(HUGO_URL, HUGO_DIRECTORY, HUGO_FILE_NAME)
    hugo_entries = build_gene_collection(hugo_string)
    tasks = []
    try:
        # Quickly get a record
        existing = EVE.get(
            endpoint="gene_symbols", token=refresh_hugo_defs.token["access_token"]
        ).json()["_items"][0]
        _id = existing["_id"]
        _etag = existing["_etag"]

        # Delete it. There is a hook in the API that responds to any completed deletion of a
        # of a record by dropping the whole collection. This is by far the easiest way to
        # update the collection, 
        EVE.delete(
            endpoint="gene_symbols",
            token=refresh_hugo_defs.token["access_token"],
            item_id=_id,
            _etag=_etag,
            code=204,
        )
        # Chunk the list to only post 10000 items at a time
        tasks = [
            chunked_upload.s(hugo_entries[x : x + 10000])
            for x in range(0, len(hugo_entries), 10000)
        ]

        result = execute_in_parallel(tasks, 700, 10)

        if not result:
            logging.error(
                {
                    "message": "Some of the hugo chunked uploads failed.",
                    "category": "ERROR-CELERY-HUGO",
                }
            )
        else:
            logging.info({"message": "Hugo gene symbols updated"})
    except RuntimeError as run:
        error = "Error adding hugo symbols to DB: %s" % str(run)
        logging.error({"message": error, "category": "ERROR-CELERY-HUGO"})


@APP.task(base=AuthorizedTask)
def check_symbols_valid(symbols: List[str]) -> dict:
    """
    Takes a list of symbols and confirms their validity.

    Arguments:
        symbols {List[str]} -- List of gene symbols.

    Returns:
        dict -- If unmatched symbols, an error message, else None.
    """
    try:
        results = EVE.get(
            endpoint="gene_symbols?where=%s" % json.dumps({"symbol": {"$in": symbols}}),
            token=check_symbols_valid.token["access_token"],
            json={},
        ).json()["_items"]
        found = [result["symbol"] for result in results]

        if len(found) == len(symbols):
            return None

        return mk_error(
            "Found invalid gene symbols: %s"
            % (", ".join([x for x in symbols if x not in found])),
            affected_paths=["ol_assay"],
        )
    except RuntimeError as run:
        error = "Error looking up hugo symbols: %s" % str(run)
        logging.error({"message": error, "category": "ERROR-CELERY-HUGO"})

        return mk_error(
            "Was unable to run gene symbol validation, please contact support",
            affected_paths=[],
        )


def get_gz_ftp(server_url: str, directory: str, file_name: str) -> str:
    """
    Fetch a .gz file from an ftp server and return in plain text.

    Arguments:
        sever_url {str} -- URL of the FTP server
        directory {str} -- Directory of file
        file_name {str} -- Name of file.

    Returns:
        str -- String contents of the file.
    """
    ftp = FTP(server_url)
    ftp.login()
    ftp.cwd(directory)
    bytes_io = BytesIO()
    ftp.retrbinary("RETR %s" % file_name, bytes_io.write)
    return str(gzip.decompress(bytes_io.getvalue()), "utf-8").strip()


def build_gene_collection(tsv: str) -> List[dict]:
    """Returns a list of dictionary entries of valid symbols.

    Arguments:
        tsv {str} -- TSV formatted string.

    Returns:
        List[dict] -- List of valid gene symbols with one key "symbol"
    """
    first_line = False
    entries = []
    for line in tsv.split("\n"):
        if line[0] == "#" and not first_line:
            first_line = True
        elif first_line:
            values = line.split("\t")
            for field in IDENTIFIER_FIELDS:
                field_value = values[field["key"]]
                if field["is_array"] and "|" in field_value:
                    entries.extend([v for v in field_value.split("|")])
                elif field_value != "-":
                    entries.append(field_value)
    return [{"symbol": entry} for entry in set(entries)]
