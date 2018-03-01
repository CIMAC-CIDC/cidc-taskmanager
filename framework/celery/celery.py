#!/usr/bin/env python
"""Configures and runs a celery app
"""

from celery import Celery

APP = Celery(
    'taskmanager',
    include=[
        'framework.tasks.cromwell_tasks', 'framework.tasks.analysis_tasks'
        ]
    )

APP.config_from_object("celeryconfig")

if __name__ == '__main__':
    APP.start()
