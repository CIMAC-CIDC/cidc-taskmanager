#!/usr/bin/env python
"""Configuration file for Celery
"""


BROKER_URL = 'amqp://localhost'
RESULT_BACKEND = 'rpc://'

TASK_SERIALIZER = 'json'
RESULT_SERIALIZER = 'json'
ACCEPT_CONTENT = ['json']
