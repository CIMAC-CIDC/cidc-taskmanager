#!/bin/bash

base_rebuild=$1

# supply anything, suggesting "1" to the command invocation to rebuild the base image.
if [ -z ${base_rebuild+x} ]; then
    docker build -t "celery-base:debian" -f BaseImage .
    docker tag celery-base gcr.io/cidc-dfci/celery-base:debian
    docker push gcr.io/cidc-dfci/celery-base:debian
fi

docker build -t "celery-taskmanager" . --no-cache
docker tag celery-taskmanager gcr.io/cidc-dfci/celery-taskmanager:dev
docker push gcr.io/cidc-dfci/celery-taskmanager:dev
