FROM gcr.io/cidc-dfci/celery-base:debian

RUN apt-get install squashfs-tools -y
COPY ./requirements.txt ./
RUN python3.6 -m pip install -r requirements.txt

COPY . /app
COPY run.sh /app/run.sh
WORKDIR /app

CMD sh run.sh