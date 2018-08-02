# FROM google/cloud-sdk:198.0.0-alpine

FROM gcr.io/cidc-dfci/celery-base

COPY ./requirements.txt ./
RUN pip3 install -r requirements.txt

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

CMD sh run.sh

