FROM python:3

COPY . /app
WORKDIR /app

RUN pip install pipenv

RUN pipenv install --system

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "-E"]
