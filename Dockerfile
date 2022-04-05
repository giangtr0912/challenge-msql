FROM python:3.8
ENV PYTHONBUFFERED 1

WORKDIR /tamara 
ADD requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip setuptools && python3 -m pip install -r requirements.txt 

ADD ./ /tamara
VOLUME ["/challenge-msql"]