#!/usr/bin/env python
"""Configuration file for Celery
"""


BROKER_URL = 'amqp://rabbitmq:5672'
RESULT_BACKEND = 'rpc://'

TASK_SERIALIZER = 'json'
RESULT_SERIALIZER = 'json'
ACCEPT_CONTENT = ['json']
