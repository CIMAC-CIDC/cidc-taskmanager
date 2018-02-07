#!/usr/bin/env python3
"""A custom class to send logs to rabbitMQ
"""

import logging


class RabbitMQHandler(logging.Handler):
    """
    Handler that sends message to RabbitMQ using kombu.
    """
    def __init__(self, uri=None, queue='logging'):
        logging.Handler.__init__(self)
        try:
            import kombu
        except ImportError:
            raise RuntimeError("Kombu required for this to work")
        if uri:
            connection = kombu.Connection(uri)
        self.queue = connection.SimpleQueue(queue)

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
