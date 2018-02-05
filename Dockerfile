FROM openjdk:alpine3.7

RUN apk add --no-cache curl
RUN apk add --no-cache bash
RUN apk add --no-cache python3
RUN apk add --no-cache python
ENV PYTHONPATH "$PYTHONPATH:/usr/lib/python3"

COPY . /app
WORKDIR /app

RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH "$PATH:/root/google-cloud-sdk/bin"

RUN gcloud auth activate-service-account --key-file=./.google_auth.json

RUN pip3 install pipenv
RUN pipenv install --system


CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "-E"]
