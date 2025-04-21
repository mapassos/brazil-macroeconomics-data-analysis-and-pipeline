#!/bin/bash
#This is a faster way to set up the Airflow pipeline for testting.


mkdir airflow && \
pip install . && \
pip install -r requirements-airflow.txt && \
eval $(cat .env) export $(cat .env | cut -d '=' -f1) && \
airflow db migrate && \
mv dags airflow

