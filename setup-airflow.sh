#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

mkdir airflow && \
pip install . && \
pip install -r requirements-airflow.txt && \
export WORK_ENV=$(pwd) && \
export AIRFLOW_HOME=$(pwd)/airflow && \
export AIRFLOW__CORE__LOAD_EXAMPLES=False &&
airflow db migrate && \
mv dags airflow

