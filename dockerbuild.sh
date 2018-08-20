#!/bin/bash

base_rebuild=$1

# supply anything, suggesting "1" to the command invocation to rebuild the base image.
if [ -z ${base_rebuild+x} ]; then
    docker build -t "celery-base" -f BaseImage .
    docker tag celery-base gcr.io/cidc-dfci/celery-base
    docker push gcr.io/cidc-dfci/celery-base
fi

docker build -t "celery-taskmanager" . --no-cache
docker tag celery-taskmanager gcr.io/cidc-dfci/celery-taskmanager:latest
docker push gcr.io/cidc-dfci/celery-taskmanager:latest
