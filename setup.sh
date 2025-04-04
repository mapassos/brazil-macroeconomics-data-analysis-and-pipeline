#THIS IS AN EASIER WAY TO SETUP FOR AIRFLOW
#

eval $(cat .env) && \
pip install -e . && \
pip install requirements_dag.py && \
airflow db migrate 

