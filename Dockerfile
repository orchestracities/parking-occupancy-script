FROM python:2.7

COPY requirements.txt /
COPY occupancy.py /

RUN pip install -r /requirements.txt

CMD /occupancy.py
