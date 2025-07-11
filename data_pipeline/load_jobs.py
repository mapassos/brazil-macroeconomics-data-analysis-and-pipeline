import pandas as pd
from .db_utils import PostgresDB, Engine

pgdb = PostgresDB()
pgdb.init_engine()

def data_modeling(path: str, schema: str) -> tuple[dict[str, pd.DataFrame]]:
    '''
    Model the data as star schema

    '''
    df = pd.read_csv(
            path, 
            delimiter = '\t',
            engine='pyarrow',
            dtype_backend='pyarrow'
    )

    if schema == 'mes':
        df['periodo_mes'] = pd.Series(
            pd.PeriodIndex(df['periodo_mes'], freq = 'M')\
                .to_timestamp(how = 'e')\
                .to_list(),
            dtype= 'timestamp[s][pyarrow]'
        )
        df['mes_numerico'] = df['periodo_mes'].dt.month
    
    #create surrogate key
    df['id_' + schema] = pd.Series(
        range(1, 1 + len(df)),
        dtype= 'int32[pyarrow]'
    )
    
    df[['ano', 'decada']] = df[['ano', 'decada']].astype('int32[pyarrow]')    

    if schema == 'mes':
        df['ipca_mes'] = df['ipca_mes'].astype('float32[pyarrow]')
        
        indicadores_fato = df[[
            'id_mes',
            'meta_acumulada_mes',
            'selic_acumulada_mes',
            'ipca_mes'
        ]]
        data_dimensao = df[[
            'id_mes',
            'periodo_mes',
            'mes_numerico',
            'mes',
            'ano',
            'decada'
        ]]
    else:
        df['ipca_acumulado_ano'] = df['ipca_acumulado_ano']\
                .astype('float32[pyarrow]')

        indicadores_fato = df[[
            'id_ano',
            'meta_acumulada_ano',
            'selic_acumulada_ano',
            'ipca_acumulado_ano'
        ]]
        data_dimensao = df[[
            'id_ano',
            'ano',
            'decada'
        ]]
    
    return {'data_dimensao': data_dimensao}, {'indicadores_fato':indicadores_fato}

def load_todb(
    engine: Engine,
    table: pd.DataFrame, 
    name: str,  
    schema: str | None = None,
    exists: bool | None = False,
    key: str | None = None) -> None:
    '''
    Load the data to the postgres db schema

    '''
    if exists:
        temp_tab = 'temp_' + name
        table.to_sql(
            name = temp_tab,
            con = engine, 
            schema = schema, 
            if_exists = 'append', 
            index = False
        )
        pgdb.sql(
            f'''MERGE INTO {schema}.{name} AS target
            USING {schema}.{temp_tab} AS temp
            ON target.{key} = temp.{key}
            WHEN NOT MATCHED THEN
            INSERT ({', '.join(table.columns)})
            VALUES ({', '.join('temp.'+col for col in table.columns)});'''
        )
        pgdb.sql(
            f'DROP TABLE {schema}.{temp_tab};'
        )
        
    else:
        table.to_sql(
            name = name, 
            con = engine, 
            schema = schema, 
            index = False
        )

def run(paths: tuple[str]) -> dict[str, tuple] :
    '''
    Run load process and return the loaded tabs for each created schemas
    '''
    engine = pgdb.engine
    loaded_tabs = {}

    for schema in paths: 
        pgdb.sql(
            f'CREATE SCHEMA IF NOT EXISTS {schema};'
        )

        tables = data_modeling(paths.get(schema), schema)
        key = 'id_' + schema

        schema_tables = pgdb.schema_tables(schema) 
        loaded_tabs[schema] = []

        for table in tables:
            for name in table:
                loaded_tabs[schema].append(name)
                if name in schema_tables:
                    load_todb(
                        table = table.get(name),
                        name = name,
                        schema = schema,
                        engine = engine,
                        exists = True,
                        key = key
                    )
                    
                else:
                    load_todb(
                        table = table.get(name),
                        name = name,
                        schema = schema,
                        engine = engine
                    )
    
                    if 'dimensao' in name:
                        pgdb.sql(
                            f'ALTER TABLE {schema}.{name}\
                            ADD PRIMARY KEY ({key});'
                        )
                    else:
                        pgdb.sql(
                            f'''ALTER TABLE {schema}.{name}
                                ADD CONSTRAINT fk_data
                                    FOREIGN KEY ({key})
                                        REFERENCES {schema}.data_dimensao({key});'''
                        )
        
    return loaded_tabs

