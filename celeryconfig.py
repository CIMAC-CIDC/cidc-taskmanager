#!/usr/bin/env python
"""
Configuration file for Celery
"""


broker_url = 'amqp://rabbitmq:5672'
result_backend = 'rpc://'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']

imports = ('Celery.tasks')
