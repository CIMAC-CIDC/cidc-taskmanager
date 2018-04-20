#!/bin/bash
docker build -t "celery-taskmanager" .
docker tag celery-taskmanager undivideddocker/celery-taskmanager
docker push undivideddocker/celery-taskmanager
