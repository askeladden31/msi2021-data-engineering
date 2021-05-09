FROM python:3.7.10

ADD msi2021-data-engineering.py .
ADD dbase.py .
ADD dextract.py .

RUN pip install boto3

CMD [ "python", "./msi2021-data-engineering.py" ]