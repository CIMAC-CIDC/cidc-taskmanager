FROM google/cloud-sdk:195.0.0-alpine

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

RUN apk add --no-cache curl
RUN apk add --no-cache bash
RUN apk add --no-cache python3
RUN apk add --no-cache python

ENV PYTHONPATH "$PYTHONPATH:/usr/lib/python3"

RUN pip3 install pipenv
RUN pipenv install --system

CMD sh run.sh
