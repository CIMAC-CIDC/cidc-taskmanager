#!/usr/bin/env python
"""
Sets some variables when the module is first run
"""
__author__ = "Lloyd McCarthy"
__license__ = "MIT"

from os import environ as env
from dotenv import load_dotenv, find_dotenv

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

AUTH0_DOMAIN = env.get("AUTH0_DOMAIN")
AUDIENCE = env.get("AUDIENCE")
CLIENT_SECRET = env.get("CLIENT_SECRET")
if CLIENT_SECRET:
    CLIENT_SECRET = CLIENT_SECRET.strip()

CLIENT_ID = env.get("CLIENT_ID")
CROMWELL_URL = None
GOOGLE_BUCKET_NAME = env.get("GOOGLE_BUCKET_NAME")
GOOGLE_UPLOAD_BUCKET = env.get("GOOGLE_UPLOAD_BUCKET")
DOMAIN = env.get("DOMAIN")
EVE_URL = None
LOGSTORE = env.get("LOGSTORE")
MANAGEMENT_API = env.get("MANAGEMENT_API")
RABBIT_MQ_URI = None
SENDGRID_API_KEY = env.get("SENDGRID_API_KEY")

if not env.get("IN_CLOUD"):
    EVE_URL = "http://localhost:5000"
    CROMWELL_URL = "http://localhost:8000"
else:
    EVE_URL = "http://%s:%s" % (
        env.get("INGESTION_API_SERVICE_HOST"),
        env.get("INGESTION_API_SERVICE_PORT"),
    )
    CROMWELL_URL = "http://%s:%s/api/workflows/v1" % ("localhost", "8000")
    RABBIT_MQ_URI = "amqp://%s:%s" % (
        env.get("RABBITMQ_SERVICE_HOST"),
        env.get("RABBITMQ_SERVICE_PORT"),
    )
