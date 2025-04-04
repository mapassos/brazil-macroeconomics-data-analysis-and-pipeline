#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

mkdir airflow && \
eval $(cat .env) && \
pip install -e . && \
pip install requirements_dag.py && \
airflow db migrate && \
mv dags airflow

