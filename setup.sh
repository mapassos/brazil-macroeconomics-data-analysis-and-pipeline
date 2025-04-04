#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

mkdir airflow && \
eval $(cat .env) && \
pip install -e . && \
pip install -r $WORK_ENV/requirements_dag.txt && \
airflow db migrate && \
mv dags airflow

