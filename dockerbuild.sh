#!/bin/bash

if [ ! -f .google_auth.json ]; then
	echo "Google authorization file not found! Please contact Lloyd for access"
	exit 1
fi

docker build -t "celery-taskmanager" .
