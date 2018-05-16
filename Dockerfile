FROM google/cloud-sdk:198.0.0-alpine

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

RUN apk add --no-cache build-base
RUN apk add --no-cache linux-headers
RUN apk add --no-cache curl
RUN apk add --no-cache bash
RUN apk add --no-cache python-dev
RUN apk add --no-cache python3-dev
RUN apk add --no-cache python3
RUN apk add --no-cache python

ENV PYTHONPATH "$PYTHONPATH:/usr/lib/python3"

RUN pip3 install -r requirements.txt

CMD sh run.sh
