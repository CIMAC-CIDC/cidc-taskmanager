#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""

import re
import subprocess
import time
from uuid import uuid4
from os import remove
from typing import List
from cidc_utils.requests import SmartFetch
from celery import group
from framework.tasks.AuthorizedTask import AuthorizedTask
from framework.celery.celery import APP
from framework.tasks.variables import EVE_URL, LOGGER

HAPLOTYPE_FIELD_NAMES = [
    'allele_group',
    'hla_allele',
    'synonymous_mutation',
    'non_coding_mutation'
]


def process_hla_file(
    hla_path: str, trial_id: str, assay_id: str, record_id: str
) -> dict:
    """
    Turns an .hla file into an array of HLA mongo entries.

    Arguments:
        hla_path {str} -- Path to input file.
        trial_id {str} -- Trial ID of run.
        assay_id {str} -- Assay ID of run.
        record_id {str} -- Mongo ID of parent record.

    Returns:
        [dict] -- Array of HLA mongo records.
    """
    hla_records = []
    with open(hla_path, 'r', 8192) as hla:
        for line in hla:
            # Split into columns
            columns = line.split()
            try:
                hla_record = {
                    "gene_name": columns[0],
                    "haplotypes": [],
                    "assay": assay_id,
                    "trial": trial_id,
                    "record_id": record_id
                }
                for i in range(1, len(columns)):
                    haplotype = columns[i]
                    haplotype_fields = haplotype.split('_')
                    haplotype_record = {}

                    # Check for suffix.
                    if len(haplotype_fields) > 1:
                        # If suffix is found, store value, then trim.
                        if not str.isdigit(haplotype_fields[-1][-1]):
                            haplotype_record['suffix'] = haplotype_fields[-1][-1]
                            haplotype_fields[-1] = haplotype_fields[-1][:-1]

                    # Add all parts of the record that are present.
                    for j in range(2, len(haplotype_fields)):
                        haplotype_record[HAPLOTYPE_FIELD_NAMES[j - 2]] = int(haplotype_fields[j])

                    hla_record['haplotypes'].append(haplotype_record)

                hla_records.append(hla_record)
            except IndexError:
                print("There was a problem with the format of the HLA file")
            except ValueError:
                print("Attempted to convert a string to an int")

    if hla_records:
        return hla_records
    return None


def process_table(
        path: str, trial_id: str, assay_id: str, record_id: str
) -> dict:
    """
    Processes any table format data assuming the first row is a header row.

    Arguments:
        path {str} -- Path to file.
        trial_id {str} -- Trial ID that file belongs to.
        assay_id {str} -- Assay ID that file belongs to.
        record_id {str} -- Mongo ID of the parent file.
        sample_id {str} -- Sample ID that run was performed on.

    Raises:
        IndexError -- [description]

    Returns:
        [dict] -- List of entries, where each row becomes a mongo record.
    """
    first_line = False
    entries = []
    with open(path, 'r', 8192) as table:
        column_headers = []
        for line in table:
            if line[0] != '#' and not first_line:
                first_line = True
                column_headers = [
                    header.strip().replace('"', '').replace('.', '') for header in line.split('\t')
                ]
            elif first_line:
                values = line.split('\t')
                if not len(column_headers) == len(values):
                    LOGGER.error("Header and value length mismatch!")
                    raise IndexError
                entries.append(
                    dict(
                        (
                            column_headers[i],
                            values[i].strip().replace('"', '')
                        ) for i, j in enumerate(values)
                    )
                )

    [entry.update(
        {
            'trial': trial_id,
            'assay': assay_id,
            'record_id': record_id
        }
    ) for entry in entries]

    if entries:
        return entries
    return None


PROC = [
    {
        're': r'[._]hla[._]',
        'func': process_hla_file,
        'endpoint': 'hla'
    },
    {
        're': r'.maf$',
        'func': process_table,
        'endpoint': 'vcf'
    },
    {
        're': r'.combined.all.binders.txt.annot.txt.clean.txt$',
        'func': process_table,
        'endpoint': 'neoantigen'
    },
    {
        're': r'^Facets_output.',
        'func': process_table,
        'endpoint': 'purity'
    },
    {
        're': r'^cluster.tsv$',
        'func': process_table,
        'endpoint': 'clonality_cluster'
    },
    {
        're': r'^loci.tsv$',
        'func': process_table,
        'endpoint': 'loci'
    },
    {
        're': r'^sample_confints_CP.',
        'func': process_table,
        'endpoint': 'confints_cp'
    },
    {
        're': r'.pyclone.tsv$',
        'func': process_table,
        'endpoint': 'pyclone'
    },
    {
        're': r'_segments',
        'func': process_table,
        'endpoint': 'cnv'
    }
]


@APP.task(base=AuthorizedTask)
def process_file(rec, pro, eve_fetcher):
    """
    Worker process that handles processing an individual file.

    Arguments:
        rec {dict} -- A record item to be processed.
        pro {dict} -- The processing type to be done on the file.
        eve_fetcher {SmartFetch} -- Eve connection instance.

    Returns:
        boolean -- Status of the operation.
    """
    gs_args = [
        'gsutil',
        'cp',
        rec['gs_uri'],
        'temp_file'
    ]
    subprocess.run(gs_args)
    try:
        # Use a random filename as tasks are executing in parallel.
        temp_file_name = str(uuid4())
        records = pro['func'](
            'temp_file',
            rec['trial']['$oid'],
            rec['assay']['$oid'],
            rec['_id']['$oid']
        )
        remove(temp_file_name)
    except OSError:
        pass
    try:
        eve_fetcher.post(
            endpoint=rec['endpoint'],
            token=process_file.token['access_token'],
            code=201,
            json=records
        )
        return True
    except RuntimeError as runt:
        print(runt)
        return False


@APP.task
def postprocessing(records: List[dict]) -> None:
    """
    Scans incoming records and sees if they need post-processing.
    If so, they are processed and then uploaded to mongo.

    Arguments:
        records {List[dict]} -- [description]

    Returns:
        None -- [description]
    """
    eve_fetcher = SmartFetch(EVE_URL)
    tasks = []

    for rec in records:
        print('Processing: ')
        print(rec['file_name'])
        for pro in PROC:
            regex = re.compile(pro['re'])
            if re.search(regex, rec['file_name']):
                # If a match is found, add to job queue
                tasks.append(
                    process_file.s(rec, pro, eve_fetcher)
                )
    job = group(tasks)

    # Start all jobs in parallel
    result = job.apply_async()
    # Wait for jobs to finish.
    while not result.ready():
        time.sleep(10)

    if not result.successful():
        print('Error, some of the tasks failed')
