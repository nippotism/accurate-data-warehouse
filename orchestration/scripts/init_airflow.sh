#!/bin/bash

if [ ! -f /opt/airflow/config/.initialized ]; then
  echo "First time init..."

  python /opt/airflow/config/init_connections.py
  python /opt/airflow/config/generate_variables.py
  airflow variables import /opt/airflow/config/variables.json

  touch /opt/airflow/config/.initialized
else
  echo "Already initialized, skip"
fi