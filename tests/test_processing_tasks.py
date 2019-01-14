"""
Tests for the processing_tasks module.
"""
from framework.tasks.processing_tasks import add_record_context
from framework.tasks.data_classes import RecordContext

def test_add_record_context():
    """
    Test for add_record_context
    """
    rec = [{}]
    context = RecordContext(trial="123", assay="456", record="foo")
    add_record_context(rec, context)
    assert rec[0] == {
        "trial": "123",
        "assay": "456",
        "record_id": "foo"
    }
