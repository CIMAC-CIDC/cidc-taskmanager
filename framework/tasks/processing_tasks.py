#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""

from typing import List
from cidc_utils.requests import SmartFetch
from AuthorizedTask import AuthorizedTask
from framework.celery.celery import APP
from variables import EVE_URL, LOGGER


def process_maf_file(maf_path: str, trial_id: str, assay_id: str) -> List(dict):
    """
    Takes a maf file and processes it into a mongo record

    Arguments:
        maf_path {str} -- path to maf file.
        trial_id {str} -- Trial ID that file belongs to
        assay_id {str} -- Assay ID that file belongs to

    Raises:
        IndexError -- [description]

    Returns:
        [dict] -- List of processed maf entries
    """
    first_line = False
    maf_entries = []
    with open(maf_path, 'r', 8192) as maf:
        column_headers = []
        for line in maf:
            if line[0] == '#':
                first_line = True
            elif first_line:
                first_line = False
                column_headers = line.split('\t')
            else:
                values = line.split('\t')
                if not len(column_headers) == len(values):
                    LOGGER.error("Header and value length mismatch!")
                    raise IndexError
                maf_entries.append(
                    dict((column_headers[i], values[i]) for i, j in enumerate(values))
                )
    (entry.update({'trial': trial_id, 'assay': assay_id}) for entry in maf_entries)
    return maf_entries


@APP.task(base=AuthorizedTask)
def parse_maf(maf_path: str, trial_id: str, assay_id: str) -> None:
    """[summary]

    Arguments:
        maf_path {str} -- [description]
        trial_id {str} -- [description]
        assay_id {str} -- [description]

    Raises:
        IndexError -- [description]
    """
    LOGGER.debug('Beginning MAF file processing')
    maf = process_maf_file(maf_path, trial_id, assay_id)
    LOGGER.debug('Processing complete, uploading results to Mongo')
    eve_fetch = SmartFetch(EVE_URL)
    try:
        results = eve_fetch.post(
            'variants', code=201, token=parse_maf.token['access_token'], json=maf)
        LOGGER.debug('Upload Completed.')
    except RuntimeError as err:
        LOGGER.error('Upload to Eve Failed!')
        LOGGER.error(err)
