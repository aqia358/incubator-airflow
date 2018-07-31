#!/bin/bash
source /etc/profile
source /data/share/AIRFLOW_VENV/bin/activate
/data/share/AIRFLOW_VENV/bin/airflow webserver -p 8080 >> /data/share/AIRFLOW_VENV/airflow/logs/airflow-webserver.log 2>&1
