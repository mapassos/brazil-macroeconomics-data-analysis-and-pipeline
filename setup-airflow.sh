#!/bin/bash
#This is a faster way to set up the Airflow pipeline for testting.


mkdir airflow && \
pip install -r airflow-requirements.txt && \
pip install apache-airflow==2.10.5 && \
eval $(cat .env) export $(cat .env | cut -d '=' -f1) && \
airflow db migrate && \
mv dags airflow

