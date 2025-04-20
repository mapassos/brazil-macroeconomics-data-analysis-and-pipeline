from setuptools import setup, find_packages

setup(
    name='etl-scripts',
    packages=find_packages(where='etl_scripts'),
    py_modules=['etl_scripts.pipeline'], 
    entry_points={
        'console_scripts': [
            'pipeline = etl_scripts.pipeline:run_pipeline',
        ]
    },
)
