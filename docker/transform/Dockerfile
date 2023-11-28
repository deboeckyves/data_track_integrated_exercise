FROM python:3.12.0

ADD transform.py .

RUN pip install boto3 requests pyspark

CMD ["python","./transform.py","-d","2023-11-23","-e","dev"]