#!/usr/bin/env python
"""
A custom class to send logs to rabbitMQ
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

import logging
import kombu


class RabbitMQHandler(logging.Handler):
    """
    Handler that sends message to RabbitMQ using kombu.
    """
    def __init__(self, uri=None, queue='logstash'):
        logging.Handler.__init__(self)
        connection = kombu.Connection(uri, connect_timeout=1)
        connection.connect()
        self.queue = connection.SimpleQueue(queue)

    def emit(self, record):
        """
        Puts the record on a logging queue.
        Arguments:
            record {[type]} -- [description]
        """
        self.queue.put(record.msg)

    def close(self):
        """
        Closes the queue.
        """
        try:
            self.queue.close()
        except AttributeError:
            return
