#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""


def parse_maf(maf_path, trial_id, assay_id):
    first_line = False
    with open(maf_path) as maf:
        for line in maf:
            maf_entry = {}
            if not first_line:
                columns = line.split()
                maf_entry = dict.fromkeys(columns)
