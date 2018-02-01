#!/usr/bin/env python
"""Configures and runs a celery app
"""

from __future__ import absolute_import, unicode_literals
from celery import Celery

APP = Celery('tasks', backend='rpc://', broker='amqp://rabbitmq:5672')

if __name__ == '__main__':
    APP.start()
