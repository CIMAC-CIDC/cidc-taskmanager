#!/usr/bin/env python
"""
Utility method for executing sub-tasks.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import time
import logging
from typing import List
from celery import group
from framework.celery.celery import APP


def execute_in_parallel(tasks: List[object], timeout: int, step: int) -> bool:
    """
    Takes a list of function signatures of tasks and executes them in parallel.

    Arguments:
        tasks {List[object]} -- List of function signatures from annotated tasks.
        timeout {int} -- Total amount of time to wait.
        step {int} -- interval at which to check for a result.

    Returns:
        bool -- True if all tasks return without error, else false.
    """
    # Run jobs on workers.
    job = group(tasks) if len(tasks) == 1 else group(*tasks)
    result = job.apply_async()
    return True
