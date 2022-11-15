FROM python:3.11-alpine

RUN apk add --no-cache postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev

RUN mkdir /app

COPY requirements.txt /app

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

COPY occupancy.py /app

ENTRYPOINT ["python"]

CMD ["/occupancy.py"]
