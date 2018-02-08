#!/usr/bin/env python3
"""
Defines tasks for receiving logs from running applications, then sending them to a central location
"""

from .celery_app import APP

import logging
import logstash
import sys
