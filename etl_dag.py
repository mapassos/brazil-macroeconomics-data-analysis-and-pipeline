from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from etl_scripts import pipeline
import os

WORK_ENV = os.getenv('WORK_ENV')

@dag(
    'etl-scrap',
    schedule = '@monthly',
    start_date = datetime(2025, 3, 20)
)
def etl():

    @task_group(group_id='extract_selic')
    def extract_selic():
        @task.bash
        def build_selic_image():
            return f'''\
            cd {WORK_ENV}
            pwd
            docker build -t scrap -f Dockerfile .
            '''

        @task.bash
        def extract_selic_tofile():
            return f'echo -e $(docker run --rm scrap) > {WORK_ENV}/dados/selic.tsv'

        build_selic_image() >> extract_selic_tofile()


    @task_group(group_id='extract_ipca')
    def extract_ipca():
        @task.bash()
        def download_ipca():
            return f'curl -o {WORK_ENV}/dados/ipca.zip https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA/Serie_Historica/ipca_SerieHist.zip'

        @task.bash()
        def get_ipca_filename():
            return f'unzip -l {WORK_ENV}/dados/ipca.zip | grep -oE ipca.*\\.xls'

        ipca_filename = get_ipca_filename()

        @task.bash()
        def unzip_ipca():
            return f'unzip -oq {WORK_ENV}/dados/ipca.zip -d {WORK_ENV}/dados'

        @task.bash()
        def rename_ipca(filename: str):
            return f'mv {WORK_ENV}/dados/{filename} {WORK_ENV}/dados/ipca.xls' 

        @task.bash()
        def remove_leftover():
            return f'rm {WORK_ENV}/dados/ipca.zip'

        download_ipca() >> ipca_filename >> unzip_ipca() >> rename_ipca(ipca_filename) >> remove_leftover()  

    @task(task_id= 'transform_load')
    def transform_and_load():
        pipeline.run_pipeline()

    [extract_selic(), extract_ipca()] >> transform_and_load() 

etl()
