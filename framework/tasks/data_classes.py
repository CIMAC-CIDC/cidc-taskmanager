#!/usr/bin/env python
"""
Module for holding NamedTuple derived classes used in more than one module.
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

from typing import NamedTuple

class RecordContext(NamedTuple):
    """
    Storage class for fields common to all records.

    Arguments:
        NamedTuple {typing.NamedTuple} -- Instance of the typing module's NamedTuple
    """

    trial: str
    assay: str
    record: str
