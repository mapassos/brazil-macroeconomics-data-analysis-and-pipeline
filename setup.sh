#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

mkdir airflow && \
pip install -e . && \
pip install -r requirements_dag.txt && \
eval $(cat .env) && \
airflow db migrate && \
mv dags airflow

