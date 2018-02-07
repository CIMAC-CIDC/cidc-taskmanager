#!/usr/bin/env python
"""Configures and runs a celery app
"""

from __future__ import absolute_import, unicode_literals
from celery import Celery

APP = Celery('taskmanager')

APP.config_from_object("Celery.celeryconfig")

if __name__ == '__main__':
    APP.start()
