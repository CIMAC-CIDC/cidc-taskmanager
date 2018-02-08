#!/usr/bin/env python3
"""A custom class to send logs to rabbitMQ2
"""

import logging
import socket
import kombu


class RabbitMQHandler(logging.Handler):
    """
    Handler that sends message to RabbitMQ using kombu.
    """
    def __init__(self, uri=None, queue='logstash'):
        logging.Handler.__init__(self)
        connection = kombu.Connection(uri)
        try:
            self.queue = connection.SimpleQueue(queue)
        except AttributeError:
            raise RuntimeError("Handler was unable to connect to MQ")

    def emit(self, record):
        """
        Puts the record on a logging queue
        Arguments:
            record {[type]} -- [description]
        """
        self.queue.put(record.msg)

    def close(self):
        """[summary]
        """
        self.queue.close()
