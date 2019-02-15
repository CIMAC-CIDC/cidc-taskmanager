#!/usr/bin/env python
"""
Helper methods for unit tests.
"""
__author__="Lloyd McCarthy"
__license__="MIT"
from typing import NamedTuple


class FakeFetcher(object):
    """
    Class to provide the .json() method for mocking http response calls.

    Arguments:
        object {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    def __init__(self, response):
        """
        Constructor method.

        Arguments:
            response {dict} -- Any valid dictionary.
        """
        self.response = response
    def json(self):
        """
        Returns the dictionary passed to it on init.

        Returns:
            dict -- dictionary.
        """
        return self.response


class FakeBlob(NamedTuple):
    """
    Simulates a GCS Blob.

    Arguments:
        NamedTuple {[type]} -- NamedTuple instance.
    """

    size: int
    name: str
    path: str
    prefix: str


class FakeBucket(object):
    """
    Simulates a GCS Bucket.

    Arguments:
        object {[type]} -- [description]
    """

    def __init__(self, bucket_name: str):
        """
        Constructor

        Arguments:
            bucket_name {str} -- Bucket name.
        """
        self.bucket_name = bucket_name

    def list_blobs(self, prefix: str = None):
        """
        Method to intercept calls to list_blobs. Returns a 1 length list.

        Keyword Arguments:
            prefix {str} -- [description] (default: {None})

        Returns:
            List[FakeBlob] -- A list with 1 fake blob in it.
        """
        if "fail" in prefix:
            return []
        return [FakeBlob("1", "FOO", "somepath", prefix)]


class FakeClient(object):
    """
    Simulates GCS Client.

    Arguments:
        object {[type]} -- [description]
    """

    def get_bucket(self, bucket_name: str):
        """
        Intercepts call to get_bucket.

        Arguments:
            bucket_name {str} -- Bucket name.

        Returns:
            FakeBucket -- Fake google bucket.
        """
        return FakeBucket(bucket_name)
