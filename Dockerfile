FROM python:3.10

ADD main.py employeeETL.py .

RUN pip install pandas pymongo numpy

COPY employee_details.csv uri_mongo.txt .

CMD ["python", "./main.py"]