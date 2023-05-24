1. Unpack the files main.py, employeeETL.py, uri_mongo.txt, employee_details.csv, Dockerfile and docker-compose.yml to one directory
Credentials for the MongoDB are provided in uri_mongo.txt for the purposes of this exercise, but in practice each engineer will be a new user with their own credentials

2. To build the Docker image, run the following in the command prompt:
    docker build -t python-img . 

3. In the command prompt, run the following: 
    docker-compose up

4. To stop the container, run:
    docker-compose down

5. The Python application will upload the data to the MongoDB.

6. A logfile will be created under ETLlog.log, which can be used for debugging and monitoring.

7. For unit testing, a seperate file unittests.py is provided.