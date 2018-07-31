#!/bin/bash
source /etc/profile
/bin/airflow flower > /var/log/airflow-flower.log 2>&1
