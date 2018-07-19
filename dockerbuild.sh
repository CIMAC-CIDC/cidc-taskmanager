#!/bin/bash

base_rebuild=$1

# supply anything, suggesting "1" to the command invocation to rebuild the base image.
if [ -z ${base_rebuild+x} ]; then
    docker build -t "celery-base" -f BaseImage
    docker tag celery-base undivideddocker/celery-base
    docker push undivideddocker/celery-base
fi

docker build -t "celery-taskmanager" .
docker tag celery-taskmanager undivideddocker/celery-taskmanager
docker push undivideddocker/celery-taskmanager
