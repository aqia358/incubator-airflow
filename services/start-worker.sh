#!/bin/bash
source /etc/profile
source /data/share/AIRFLOW_VENV/bin/activate
/data/share/AIRFLOW_VENV/bin/airflow worker > /data/share/AIRFLOW_VENV/airflow/logs/airflow-worker.log 2>&1
