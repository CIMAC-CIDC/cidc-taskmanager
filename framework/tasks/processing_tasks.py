#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""

import re
import subprocess
from os import remove
from typing import List
from cidc_utils.requests import SmartFetch
from AuthorizedTask import AuthorizedTask
from framework.celery.celery import APP
from variables import EVE_URL, LOGGER


def process_maf_file(
        maf_path: str, trial_id: str, assay_id: str, record_id: str
) -> List(dict):
    """
    Takes a maf file and processes it into a mongo record

    Arguments:
        maf_path {str} -- path to maf file.
        trial_id {str} -- Trial ID that file belongs to
        assay_id {str} -- Assay ID that file belongs to
        record_id {str} -- ID of data entry that matches the MAF file.

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

    (entry.update(
        {'trial': trial_id, 'assay': assay_id, 'data_id': record_id}
    ) for entry in maf_entries)

    return maf_entries


@APP.task(base=AuthorizedTask)
def parse_maf(records: List[dict]) -> None:
    """
    Examines a newly inserted record

    Arguments:
        maf_record
    Raises:
        IndexError -- [description]
    """

    for maf_record in records:
        LOGGER.debug('Beginning MAF file processing')
        logstring = 'Record, ' + maf_record['_id'] + 'recieved'
        LOGGER.debug(logstring)

        # Check If MAF
        maf_re = re.compile(r'.maf$')
        if not re.search(maf_re, maf_record['file_name']):
            return

        LOGGER.debug('Identified record as MAF file, converting to VCF')

        # Copy to local disk
        gs_args = [
            'gsutil',
            'cp',
            maf_record['gs_uri'],
            '.'
        ]
        subprocess.run(gs_args)

        # Process
        maf_entries = process_maf_file(
            maf_record['file_name'],
            maf_record['trial_id'],
            maf_record['assay_id'],
            maf_record['_id']
        )

        LOGGER.debug('Processing complete.')

        # Clean up
        try:
            remove(maf_record['file_name'])
        except OSError:
            pass

        eve_fetcher = SmartFetch(EVE_URL)

        LOGGER.debug('Uploading data.')
        # Insert data
        try:
            eve_fetcher.post(
                endpoint='vcf',
                code=201,
                token=parse_maf.token['access_token'],
                json=maf_entries
            )
            LOGGER.debug('Upload Succesful')
        except RuntimeError as runt:
            msg = 'Upload failed: ' + runt
            LOGGER.error(msg)
