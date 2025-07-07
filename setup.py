from setuptools import setup, find_packages

setup(
    name='data-pipeline',
    packages=find_packages(include=['data_pipeline']),
    install_requires=[
        'pandas==2.0.0',
        'numpy==1.26.1',
        'xlrd==2.0.1',
        'sqlalchemy==1.4.54',
        'psycopg2==2.9.10',
        'pyarrow==19.0.1'
    ],
)
