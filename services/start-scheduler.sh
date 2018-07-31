#!/bin/bash
source /etc/profile
source /data/share/AIRFLOW_VENV/bin/activate
/data/share/AIRFLOW_VENV/bin/airflow scheduler > /data/share/AIRFLOW_VENV/airflow/logs/airflow-scheduler.log 2>&1
