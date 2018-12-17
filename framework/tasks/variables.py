"""
Sets some variables when the module is first run
"""
from os import environ as env
from dotenv import load_dotenv, find_dotenv

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

AUTH0_DOMAIN = env.get("AUTH0_DOMAIN")
AUDIENCE = env.get("AUDIENCE")
CLIENT_SECRET = env.get("CLIENT_SECRET").strip()
CLIENT_ID = env.get("CLIENT_ID")
CROMWELL_URL = None
GOOGLE_BUCKET_NAME = env.get("GOOGLE_BUCKET_NAME")
GOOGLE_UPLOAD_BUCKET = env.get("GOOGLE_UPLOAD_BUCKET")
DOMAIN = env.get("DOMAIN")
EVE_URL = None
LOGSTORE = env.get("LOGSTORE")
MANAGEMENT_API = env.get("MANAGEMENT_API")

if not env.get("IN_CLOUD"):
    EVE_URL = "http://localhost:5000"
    CROMWELL_URL = "http://localhost:8000"
else:
    EVE_URL = "http://%s:%s" % (
        env.get("INGESTION_API_SERVICE_HOST"),
        env.get("INGESTION_API_SERVICE_PORT"),
    )
    CROMWELL_URL = "http://%s:%s/api/workflows/v1" % (
        "localhost",
        "8000"
    )
