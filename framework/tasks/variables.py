"""
Sets some variables when the module is first run
"""
from os import environ as env
from dotenv import load_dotenv, find_dotenv

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

DOMAIN = env.get('DOMAIN')
CLIENT_SECRET = env.get('CLIENT_SECRET')
CLIENT_ID = env.get('CLIENT_ID')
AUDIENCE = env.get('AUDIENCE')
MANAGEMENT_API = env.get('MANAGEMENT_API')

EVE_URL = None
CROMWELL_URL = None

if not env.get('IN_CLOUD'):
    EVE_URL = 'http://localhost:5000'
    CROMWELL_URL = 'http://localhost:8000'
else:
    EVE_URL = (
        'http://' +
        env.get('INGESTION_API_SERVICE_HOST') + ':' + env.get('INGESTION_API_SERVICE_PORT')
    )
    CROMWELL_URL = (
        'http://' +
        env.get('CROMWELL_SERVER_SERVICE_HOST') + ':' + env.get('CROMWELL_SERVER_SERVICE_PORT') +
        '/api/workflows/v1'
    )
