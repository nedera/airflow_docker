
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
$ docker compose up airflow-init
$ docker compose up
```
## Accessing the web interface
```
http://localhost:8080
```
## Cleaning up
```
$ docker compose down --volumes --rmi all
```

