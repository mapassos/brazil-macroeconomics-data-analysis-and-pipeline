from setuptools import setup, find_packages

setup(
    name='etl-scripts',
    packages=find_packages(include=['etl_scripts']),
    entry_points={
        'console_scripts': [
            'etl_scripts-pipeline = etl_scripts.pipeline:run_pipeline',
        ]
    },
    install_requires=[
        'pandas==2.0.0',
        'numpy==1.26.1',
        'xlrd==2.0.1'
    ],
)
