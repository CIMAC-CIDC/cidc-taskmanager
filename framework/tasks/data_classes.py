"""
Module for holding NamedTuple derived classes used in more than one module.
"""
from typing import NamedTuple

class RecordContext(NamedTuple):
    """
    Storage class for fields common to all records.

    Arguments:
        NamedTuple {[type]} -- [description]
    """

    trial: str
    assay: str
    record: str
