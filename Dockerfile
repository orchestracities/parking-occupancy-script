FROM python:2.7-alpine

RUN mkdir /app

COPY requirements.txt /app

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

COPY occupancy.py /app

ENTRYPOINT ["python"]

CMD ["/occupancy.py"]
