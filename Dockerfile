FROM google/cloud-sdk:198.0.0-alpine

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

ENV PYTHONPATH "$PYTHONPATH:/usr/lib/python3"
RUN apk add --no-cache shadow \
    build-base \
    linux-headers \
    curl \
    bash \
    python-dev \
    python3-dev \
    python3 \
    python \
    && pip3 install -r requirements.txt 

RUN groupadd -g 1000 appuser && \
        useradd -r -u 1000 -g appuser appuser -d /home/appuser

RUN mkdir -p /home/appuser/.gcloud/config \
    && chown -R appuser /home/appuser/.gcloud/config
USER appuser
CMD sh run.sh
