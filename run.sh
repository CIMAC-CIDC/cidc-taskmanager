#!/bin/bash

gcloud auth activate-service-account --key-file=../auth/.google_auth.json
celery -A framework.celery.celery worker --concurrency=10 --loglevel=info -E --beat


