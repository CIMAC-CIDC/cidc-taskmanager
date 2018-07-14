FROM google/cloud-sdk:198.0.0-alpine

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

RUN apk add --no-cache shadow \
    build-base \
    linux-headers \
    curl \
    bash \
    python3-dev \
    python3 \
    && pip3 install -r requirements.txt 

CMD sh run.sh

