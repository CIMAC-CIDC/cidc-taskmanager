"""
Test module for celery functions
"""
from framework.tasks.process_npx import diff_fields

def test_diff_fields():
    """
    Test for diff_fields
    """
    actual = ['a', 'b', 'c', 'e']
    expected = ['a', 'b', 'c', 'd']
    diff = diff_fields(actual, expected)
    assert diff == ['e']
