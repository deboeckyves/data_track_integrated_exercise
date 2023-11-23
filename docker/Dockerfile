FROM python:3.12.0

ADD ingest.py .

RUN pip install boto3 requests

CMD ["python","./ingest.py","-d","2023-11-23","-e","dev"]