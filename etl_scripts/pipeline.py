from __future__ import annotations
import pandas as pd
import numpy as np
import os


def read_selic(path: str) -> pd.DataFrame:
    SELIC_COLS =[
        'reuniao_num',
        'reuniao_data',
        'reuniao_vies',
        'periodo_vigencia',
        'meta_selic_pctaa',
        'tban_pctam',
        'taxa_selic_pct',
        'taxa_selic_pctaa'
    ]

    selic_df = pd.read_csv(
            path, 
            delimiter = '\t', 
            header = None, 
            names = SELIC_COLS
    )
    
    return selic_df

def read_ipca(path: str) -> pd.DataFrame:
    IPCA_COLS = [
        'ano',
        'mes',
        'ipca_numero_indice',
        'ipca_var_mensal',
        'ipca_var_trimestral',
        'ipca_var_semetral',
        'ipca_no_ano',
        'ipca_acumulado_ano'
    ]

    ipca_df = pd.read_excel(
        path,
        names = IPCA_COLS
    ) 

    ipca_df = ipca_df[ipca_df.isnull().sum(axis = 'columns') <= 1]\
            .reset_index(drop=True)

    return ipca_df


def transform_selic(selic_df: pd.DataFrame, filter_df: pd.DataFrame) -> tuple(pd.DataFrame, pd.Dataframe):
    #read feriados_file
    feriados_df = filter_df

    #standardizing the column before the split
    selic_df.at[0, 'periodo_vigencia'] = selic_df.at[0,'periodo_vigencia'] + ' '

    #splitting column into two and drop original column
    selic_df[['vigencia_inicio', 'vigencia_fim']] = selic_df['periodo_vigencia']\
            .str.split(' - ', expand =  True)
    selic_df = selic_df.drop(['periodo_vigencia'], axis = 1)
    
    #setting up important variables
    SELIC_COLS = selic_df.columns
    NUM_COLS = len(SELIC_COLS)

    #adding current day to proper change type
    selic_df.at[0, 'vigencia_fim'] = pd.Timestamp.today()\
            .strftime('%d/%m/%Y')
    
    #setting_types
    for ii in range(1, NUM_COLS):
        if ii in range(3, 7):
            selic_df[SELIC_COLS[ii]] = selic_df[SELIC_COLS[ii]]\
                    .str.replace(',', '.') 
            selic_df[SELIC_COLS[ii]] = pd.to_numeric(
                    selic_df[SELIC_COLS[ii]]
            )
        elif ii == 2:
            continue
        else:
            selic_df[SELIC_COLS[ii]] = pd.to_datetime(
                    selic_df[SELIC_COLS[ii]], 
                    format="%d/%m/%Y"
            )

    #prior to 1993-03-04 there was no meta selic
    selic_df.loc[selic_df['reuniao_data'] < '1999-03-04', 'meta_selic_pctaa'] = np.nan
    #drop row for change that didnt come into efect 
    selic_df = selic_df.drop(selic_df[selic_df['reuniao_data'] ==  '1997-10-22'].index, axis = 0)

    #resampling to daily
    resampledselic_df = selic_df[['vigencia_inicio', 'taxa_selic_pctaa', 'meta_selic_pctaa']]\
            .set_index('vigencia_inicio')\
            .apply(lambda x: np.power(1 + x/100, 1/252))\
            .resample('B')\
            .ffill()

    #setting types
    for feriado in feriados_df:
        feriados_df[feriado] = pd.to_datetime(feriados_df[feriado])
    
    #converting to DatetimeIndex
    feriados_df = feriados_df.unstack().to_frame().set_index(0)

    #upsampling
    selicmensal_df = resampledselic_df[~resampledselic_df.index.isin(feriados_df.index)]\
        .resample('M')\
        .prod()\
        .apply(lambda x: (x - 1) * 100)
    selicmensal_df = selicmensal_df.iloc[:-1, :]

    #setting to period and renaming
    selicmensal_df.index = selicmensal_df.index.to_period()
    selicmensal_df = selicmensal_df.reset_index()
    selicmensal_df = selicmensal_df.rename(columns = {
        'vigencia_inicio': 'periodo_mes', 
        'meta_selic_pctaa': 'meta_acumulada_nomes', 
        'taxa_selic_pctaa': 'selic_acumulada_nomes'
    })   
    
    #0 to nan
    selicmensal_df['meta_acumulada_nomes'] = selicmensal_df['meta_acumulada_nomes'].replace(0, np.nan)
    

    #multiplying each factor for a month 
    selicnoano_df = resampledselic_df[~resampledselic_df.index.isin(feriados_df.index)]\
        .resample('Y')\
        .prod()\
        .apply(lambda x: (x - 1) * 100)
    selicnoano_df = selicnoano_df.reset_index()

    #rename cols
    selicnoano_df = selicnoano_df.rename(columns = {
        'vigencia_inicio': 'ano',
        'taxa_selic_pctaa': 'selic_acumulada_noano',
        'meta_selic_pctaa' : 'meta_acumulada_noano'
    })


    selicnoano_df['ano'] = selicnoano_df['ano'].dt.year
    selicnoano_df['meta_acumulada_nomes'] = selicmensal_df['meta_acumulada_nomes'].replace(0, np.nan)

    return selicmensal_df, selicnoano_df

def transform_ipca(ipca_df: pd.DataFrame) -> tuple(pd.DataFrame, pd.DataFrame):
    MESES_NUMCAP = {
        'JAN': '1',
        'FEV': '2',
        'MAR': '3',
        'ABR': '4',
        'MAI': '5',
        'JUN': '6',
        'JUL': '7',
        'AGO': '8',
        'SET': '9',
        'OUT': '10',
        'NOV': '11',
        'DEZ': '12'
    }
    
    #setting types
    ipca_df['ano'] = ipca_df['ano'].ffill().astype('str')
    
    #month to numeric
    ipca_df['mes_num'] = ipca_df['mes'].map(MESES_NUMCAP)

    #formating for type change PeriodIndex
    ipca_df['periodo_mes'] = ipca_df['ano'] + '-' + ipca_df['mes_num']

    #settign format PeriodIndex
    ipca_df['periodo_mes'] = pd.PeriodIndex(ipca_df['periodo_mes'], freq='M')
    
    #setting the numeric types
    for ii in range(len(ipca_df.columns) - 1):
        if ii == 1:
            continue
        ipca_df[ipca_df.columns[ii]] = pd.to_numeric(
                ipca_df[ipca_df.columns[ii]]
        )

    #choose features of interest
    ipca_df = ipca_df[['mes', 'periodo_mes', 'ano', 'ipca_no_ano', 'ipca_var_mensal']].copy()

    #create a new column: decada
    ipca_df.loc[:, 'decada'] = (np.floor(ipca_df.loc[:, 'ano'] / 10) * 10).astype('int')

    #create yearly df
    ipca_noano_df = ipca_df.groupby('ano')[['ipca_no_ano', 'decada']]\
            .last().reset_index()

    return ipca_df, ipca_noano_df

def merge_dfs(
    df_left: pd.DataFrame, 
    df_right: pd.DataFrame, 
    link_col: str
) -> pd.DataFrame:
    
    merged_df = df_left.merge(df_right, how='inner', on=link_col)
    
    return merged_df

def save_to_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, sep = '\t', index = False)

def run_pipeline():
    WORK_ENV = os.getenv('WORK_ENV')

    DATA_PATH = os.path.join(WORK_ENV, 'dados')

    selic_df = read_selic(
        path = os.path.join(DATA_PATH, 'selic.tsv')
    )

    ipca_df = read_ipca(
        path = os.path.join(DATA_PATH, 'ipca.xls')
    )

    feriados_df = pd.read_csv(os.path.join(DATA_PATH, 'feriados.csv'))

    selic_df, selic_ano_df = transform_selic(selic_df, feriados_df)
    ipca_df, ipca_ano_df = transform_ipca(ipca_df)

    df_mensal = merge_dfs(selic_df, ipca_df, 'periodo_mes')
    df_anual = merge_dfs(selic_ano_df, ipca_ano_df, 'ano')

    save_to_csv(df_mensal, os.path.join(DATA_PATH, 'selic_ipca_mensal.tsv'))
    save_to_csv(df_anual, os.path.join(DATA_PATH, 'selic_ipca_noano.tsv'))


if __name__ == '__main__':
    run_pipeline()
