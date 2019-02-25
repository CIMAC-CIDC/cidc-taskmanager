#!/bin/bash

gcloud auth activate-service-account --key-file=../auth/.google_auth.json
cp /root/.kube/config /config
celery -A framework.celery.celery worker --concurrency=10 --loglevel=info -E --beat