#!/usr/bin/env python
"""
Configuration file for Celery
"""

from os import environ as env

broker_url = None

if not env.get('IN_CLOUD'):
    broker_url = 'amqp://rabbitmq:5672'
else:
    print('not in the cloud')
    broker_url = (
        'amqp://' + env.get('RABBITMQ_SERVICE_HOST') + ':' + env.get('RABBITMQ_SERVICE_PORT'))

result_backend = 'rpc://'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
