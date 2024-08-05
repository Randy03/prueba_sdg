#!/bin/bash
set -e

docker run -p 8090:8080 --name airflow -e LOAD_EX=y -v /home/rame/airflow/dags:/opt/airflow/dags airflowcustom bash -c "airflow db init && airflow webserver" -e _AIRFLOW_WWW_USER_USERNAME='tomas' -e _AIRFLOW_WWW_USER_PASSWORD='123' -e _AIRFLOW_WWW_USER_FIRSTNAME='tomas' -e _AIRFLOW_WWW_USER_LASTNAME='r' -e _AIRFLOW_WWW_USER_EMAIL='email@email.com'
