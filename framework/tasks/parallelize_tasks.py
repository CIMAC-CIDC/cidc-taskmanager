#!/usr/bin/env python
"""
Utility method for executing sub-tasks.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

from typing import List
from celery import group


def execute_in_parallel(tasks: List[object]) -> bool:
    """
    Takes a list of function signatures of tasks and executes them in parallel.

    Arguments:
        tasks {List[object]} -- List of function signatures from annotated tasks.
        timeout {int} -- Total amount of time to wait.
        step {int} -- interval at which to check for a result.

    Returns:
        bool -- True if all tasks return without error, else false.
    """
    job = group(tasks) if len(tasks) == 1 else group(*tasks)
    job.apply_async()
