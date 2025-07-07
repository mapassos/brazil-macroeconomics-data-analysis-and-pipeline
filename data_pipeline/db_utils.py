from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL
from sqlalchemy.engine.base import Engine
import os


class PostgresDB():
    def __init__(self):
        self.url = URL.create(
            drivername = 'postgresql',
            username =  os.getenv('POSTGRES_USER'),
            password = os.getenv('POSTGRES_PASSWORD'),
            host = os.getenv('POSTGRES_HOST'),
            port = os.getenv('POSTGRES_PORT'),
            database = os.getenv('POSTGRES_DB')
        )
    
    def init_engine(self):
        self.engine = create_engine(
            self.url,
            client_encoding ='utf8',
            future = True
        )
        return self.engine
        

    def run_statement(self, statement: str):
        with self.engine.begin() as con:
            con.exec_driver_sql(
                statement
            )

    def schema_tables(self, schema: str) -> list[str]:
        inspector = inspect(self.engine)
        schema_tables = inspector.get_table_names(schema)
        return schema_tables

        

