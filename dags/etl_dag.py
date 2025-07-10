from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group

from data_pipeline import transform_jobs, load_jobs
from data_pipeline.db_utils import PostgresDB
from io import StringIO
import pandas as pd
import boto3
import os

WORK_ENV = os.getenv('WORK_ENV')
BUCKET_NAME = ''
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')


@dag(
    'etl-scrap',
    schedule = '@monthly',
    start_date = datetime(2025, 6, 10),
    catchup=False,
)
def etl():

    @task.bash(task_id='extract_selic')
    def extract_selic():
        return f'echo -e $(docker run --rm mapass/selic-scrapper) > {WORK_ENV}/data/selic.tsv'

    @task_group(group_id='extract_ipca')
    def extract_ipca():
        @task.bash()
        def download_ipca():
            return f'curl -o {WORK_ENV}/data/ipca.zip https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA/Serie_Historica/ipca_SerieHist.zip'

        @task.bash()
        def get_ipca_filename():
            return f'unzip -l {WORK_ENV}/data/ipca.zip | grep -oE ipca.*\\.xls'

        ipca_filename = get_ipca_filename()

        @task.bash()
        def unzip_ipca():
            return f'unzip -oq {WORK_ENV}/data/ipca.zip -d {WORK_ENV}/data'

        @task.bash()
        def rename_ipca(filename: str):
            return f'mv {WORK_ENV}/data/{filename} {WORK_ENV}/data/ipca.xls' 

        @task.bash()
        def remove_leftover():
            return f'rm {WORK_ENV}/data/ipca.zip'

        download_ipca() >> ipca_filename >> unzip_ipca() >> rename_ipca(ipca_filename) >> remove_leftover()  

    @task(task_id= 'transform')
    def transform():
        return transform_jobs.run()

    transformed_data_paths = transform() 

    @task.short_circuit
    def check_aws_credentials():
        return all((
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_DEFAULT_REGION,
            BUCKET_NAME
        ))

        
    @task(task_id='load')
    def load(paths: dict[str, str]):
        loaded_schemas = load_jobs.run(paths)

        s3 = boto3.resource(
            "s3",
            aws_access_key_id= AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
            region_name = AWS_DEFAULT_REGION
        )

        pgdb = PostgresDB()
        engine = pgdb.init_engine()

        for schema in loaded_schemas:
            for table in loaded_schemas.get(schema):
                buffer = StringIO()
                df = pd.read_sql_query(
                    f'SELECT * FROM {schema}.{table}',
                    con = engine
                )
                df.to_csv(buffer, index = False)
                s3.Object(
                    BUCKET_NAME, 
                    f'{schema}/{table}.csv'
                ).put(Body = buffer.getvalue())

    extract_selic() >> extract_ipca() >> transformed_data_paths >> check_aws_credentials() >> load(transformed_data_paths)


etl()

