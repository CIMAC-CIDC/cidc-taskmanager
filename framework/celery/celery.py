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
        'framework.tasks.processing_tasks',
        'framework.tasks.administrative_tasks'
    ]
)


APP.config_from_object("celeryconfig")


@APP.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # This is needed for celery to actually see what modules are registered to it.
    APP.loader.import_default_modules()
    check_last_login = APP.tasks['framework.tasks.administrative_tasks.check_last_login']
    poll_auth0_logs = APP.tasks['framework.tasks.administrative_tasks.poll_auth0_logs']
    logging.info({
        'message': 'periodic tasks scheduler fired',
        'category': 'INFO-CELERY-DEBUG'
    })
    # Check for user expirey once per day, check for auth0 logs every 5 minutes
    sender.add_periodic_task(86400, check_last_login.s())
    sender.add_periodic_task(300, poll_auth0_logs.s())


if __name__ == '__main__':
    APP.start()
