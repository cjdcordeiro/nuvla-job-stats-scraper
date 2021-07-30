FROM python:3-alpine

RUN pip install nuvla-api prometheus-client

WORKDIR /opt/

COPY scraper.py .

ENTRYPOINT ["./scraper.py"]