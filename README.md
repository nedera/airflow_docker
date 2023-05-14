
# Airflow with Docker (Ubuntu)




## Set up
Docker
```
$ curl -fsSL https://get.docker.com | bash
$ sudo chmod 777 /var/run/docker.sock
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml'
```
Python-venv and lib
```
$ python3.7 -m venv env
$ source env/bin/activate
$ pip install "apache-airflow[celery]==2.6.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.7.txt"
```
## Running Airflow
```
$ export AIRFLOW_UID=50000
$ docker-compose up airflow-init -d
$ docker-compose up -d
```
## Accessing the web interface
```
http://localhost:8080
```
## Cleaning up
```
$ docker-compose down --volumes --rmi all
```
## Dependencies
Create Dockerfile, requirements.txt. And run this command (1st Way)
```
$ docker build . --tag extending_airflow:latest
```
Clone airflow git-hub, add requirements.txt (folder after clone), Run command (2nd way)
```
$ git clone https://github.com/apache/airflow.git
$ sudo docker build . --build-arg AIRFLOW_VERSION='2.6.0' --tag customising_airflow:lastest

```
Change Docker-compose 
```
image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.0} (origin)
image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest} (1st way)
image: ${AIRFLOW_IMAGE_NAME:-customising_airflow:lastest} (2nd way)
```
Run this for refresh denpendency
```
$ docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
```

