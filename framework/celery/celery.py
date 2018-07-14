#!/usr/bin/env python
"""Configures and runs a celery app
"""
import logging
from cidc_utils.loghandler import StackdriverJsonFormatter
from celery import Celery


LOGGER = logging.getLogger()
LOGGER.setLevel('INFO')
LOGHANDLER = logging.StreamHandler()
FORMATTER = StackdriverJsonFormatter()
LOGHANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(LOGHANDLER)
LOGGER.info({
    'message': 'Testing logging',
    'category': 'INFO-CELERY'})
APP = Celery(
    'taskmanager',
    include=[
        'framework.tasks.cromwell_tasks',
        'framework.tasks.analysis_tasks',
        'framework.tasks.processing_tasks'
    ]
)

APP.config_from_object("celeryconfig")

if __name__ == '__main__':
    APP.start()
