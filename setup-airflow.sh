#This is a faster way to set up the Airflow pipeline.


mkdir airflow && \
pip install -e . && \
pip install -r requirements-airflow.txt && \
export WORK_ENV=$(pwd) && \
export AIRFLOW_HOME=$(pwd)/airflow && \
export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
airflow db migrate && \
mv dags airflow

