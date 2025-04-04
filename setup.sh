#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

mkdir airflow && \
pip install -e . && \
pip install -r requirements_dag.txt && \
eval $(cat .env) && \
export AIRFLOW__CORE__LOAD_EXAMPLES=False &&
airflow db migrate && \
mv dags airflow

