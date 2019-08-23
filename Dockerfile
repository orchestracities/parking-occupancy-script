FROM python:2.7

ADD occupancy.py /occupancy.py

RUN pip install crate

ENTRYPOINT /occupancy.py
