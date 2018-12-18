"""
Test module for celery functions
"""
import unittest
import os
import shutil
from openpyxl.utils.exceptions import InvalidFileException
from framework.tasks.process_npx import (
    diff_fields,
    mk_error,
    add_file_extension,
    lazy_remove,
    bad_xlsx,
)


def test_bad_xlsx():
    """
    test bad_xlsx
    """
    error_list = []
    error = InvalidFileException("some_error_message")
    bad_xlsx(error_list, "some_path", error)
    assert error_list == [
        {
            "explanation": "The file was not recognized as a valid xlsx sheet and could not be"
            + " loaded.",
            "affected_paths": [],
            "raw_or_parse": "RAW",
            "severity": "CRITICAL",
        }
    ]


def test_diff_fields():
    """
    Test for diff_fields
    """
    actual = ["a", "b", "c", "e"]
    expected = ["a", "b", "c", "d"]
    diff = diff_fields(actual, expected)
    assert diff == ["e"]


def test_mk_error():
    """Test for mk_error
    """
    expected = {
        "explanation": "abcde",
        "affected_paths": ["a", "b", "c"],
        "raw_or_parse": "PARSE",
        "severity": "WARNING",
    }
    actual = mk_error("abcde", ["a", "b", "c"])
    assert actual == expected


class FileOpTestCase(unittest.TestCase):
    """
    Test case for tests that involve moving files.

    Arguments:
        unittest {unittest.TestCase} -- Base unittest.TestCase
    """

    def setUp(self):
        self.test_dir = "./test_directory"
        self.test_file_path = "%s/test_file" % self.test_dir
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)

        with open(self.test_file_path, "w") as t_f:
            t_f.write("This is a test file.")

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_add_file_extension(self):
        """
        Tests add_file_extension
        """
        self.assertTrue(os.path.exists(add_file_extension(self.test_file_path, "txt")))

    def test_lazy_remove(self):
        """
        Tests lazy_remove
        """
        with self.subTest():
            self.assertIsNone(lazy_remove(self.test_file_path))
        with self.subTest():
            self.assertRaises(FileNotFoundError, lazy_remove("nonsense_path"))
