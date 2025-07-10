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
    #set name to filter file (feriados)
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

    #drop row for selic rate that didnt come into effect 
    selic_df = selic_df.drop(selic_df[selic_df['reuniao_data'] ==  '1997-10-22'].index, axis = 0)

    #upsampling to daily
    resampled_selic_df = selic_df[['vigencia_inicio', 'taxa_selic_pctaa', 'meta_selic_pctaa']]\
            .set_index('vigencia_inicio')\
            .apply(lambda x: np.power(1 + x/100, 1/252))\
            .resample('B')\
            .ffill()

    #setting types
    for feriado in feriados_df:
        feriados_df[feriado] = pd.to_datetime(feriados_df[feriado])
    
    #getting a dataframe of DatetimeIndex dates
    feriados_df = feriados_df.unstack().to_frame().set_index(0)

    #downsampling to month
    selic_mensal_df = resampled_selic_df[~resampled_selic_df.index.isin(feriados_df.index)]\
        .resample('M')\
        .prod()\
        .apply(lambda x: (x - 1) * 100)
    selic_mensal_df = selic_mensal_df.iloc[:-1, :]

    #setting to period and renaming cols
    selic_mensal_df.index = selic_mensal_df.index.to_period()
    selic_mensal_df = selic_mensal_df.reset_index()
    selic_mensal_df = selic_mensal_df.rename(columns = {
        'vigencia_inicio': 'periodo_mes', 
        'meta_selic_pctaa': 'meta_acumulada_mes', 
        'taxa_selic_pctaa': 'selic_acumulada_mes'
    })   
    
    #limiting by only closed months
    selic_mensal_df = selic_mensal_df.iloc[:-1, :]

    #0 to nan
    selic_mensal_df['meta_acumulada_mes'] = selic_mensal_df['meta_acumulada_mes'].replace(0, np.nan)
    
    

    '''---------YEAR DATASET----------'''

    #downsampling to yearly data
    selic_anual_df = resampled_selic_df[~resampled_selic_df.index.isin(feriados_df.index)]\
        .resample('Y')\
        .prod()\
        .apply(lambda x: (x - 1) * 100)
    selic_anual_df = selic_anual_df.reset_index()

    #rename cols
    selic_anual_df = selic_anual_df.rename(columns = {
        'vigencia_inicio': 'ano',
        'taxa_selic_pctaa': 'selic_acumulada_ano',
        'meta_selic_pctaa' : 'meta_acumulada_ano'
    })

    #create column ano (year)
    selic_anual_df['ano'] = selic_anual_df['ano'].dt.year

    #limit the data to only closed years
    selic_anual_df = selic_anual_df.iloc[:-1, :]

    #replace zeros 
    selic_anual_df['meta_acumulada_ano'] = selic_anual_df['meta_acumulada_ano'].replace(0, np.nan)

    return selic_mensal_df, selic_anual_df

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
    
    #filling and setting type str
    ipca_df['ano'] = ipca_df['ano'].ffill().astype('str')
    
    #month to numeric
    ipca_df['mes_num'] = ipca_df['mes'].map(MESES_NUMCAP)

    #formating for type change PeriodIndex
    ipca_df['periodo_mes'] = ipca_df['ano'] + '-' + ipca_df['mes_num']

    #setting format to PeriodIndex
    ipca_df['periodo_mes'] = pd.PeriodIndex(ipca_df['periodo_mes'], freq='M')
    
    #setting the numeric types
    for ii in range(len(ipca_df.columns) - 1):
        if ii == 1:
            continue
        ipca_df[ipca_df.columns[ii]] = pd.to_numeric(
                ipca_df[ipca_df.columns[ii]]
        )

    #changing ipca_var_mensal column name
    ipca_df = ipca_df.rename(columns =  {
        'ipca_var_mensal' : 'ipca_mes'
    })

    #create decada column
    ipca_df['decada'] = (np.floor(ipca_df['ano'] / 10) * 10).astype('int')

    #choose feature of interest for monthly df in right order
    ipca_mensal_df = ipca_df[['periodo_mes', 'mes', 'ano', 'decada' ,'ipca_mes']]
    

    '''--------------YEAR VERSION---------------'''

    #choose features of interest for annual df in right order
    ipca_anual_df = ipca_df[['periodo_mes', 'mes', 'ano', 'decada', 'ipca_acumulado_ano', 'ipca_mes']].copy()

    #create yearly df
    ipca_anual_df = ipca_anual_df.groupby('ano')[['ipca_acumulado_ano', 'decada']]\
            .last().reset_index()

    return ipca_mensal_df, ipca_anual_df

def merge_dfs(
    df_left: pd.DataFrame, 
    df_right: pd.DataFrame, 
    link_col: str
) -> pd.DataFrame:    
    merged_df = df_left.merge(df_right, how='inner', on=link_col)

    return merged_df

def save_to_csv(df: pd.DataFrame, path: str, sep: str):
    df.to_csv(path, sep = sep, index = False)

def run() -> dict[str, str]:
    WORK_ENV = os.getenv('WORK_ENV')

    DATA_PATH = os.path.join(WORK_ENV, 'data')

    selic_df = read_selic(
        path = os.path.join(DATA_PATH, 'selic.tsv')
    )

    ipca_df = read_ipca(
        path = os.path.join(DATA_PATH, 'ipca.xls')
    )

    feriados_df = pd.read_csv(os.path.join(DATA_PATH, 'feriados.csv'))

    selic_mensal_df, selic_anual_df = transform_selic(selic_df, feriados_df)
    ipca_mensal_df, ipca_anual_df = transform_ipca(ipca_df)

    df_mensal = merge_dfs(selic_mensal_df, ipca_mensal_df, 'periodo_mes')
    df_anual = merge_dfs(selic_anual_df, ipca_anual_df, 'ano')

    df_mensal = df_mensal[[
        'periodo_mes', 
        'mes', 
        'ano', 
        'decada', 
        'meta_acumulada_mes', 
        'selic_acumulada_mes', 
        'ipca_mes'
    ]]    

    df_anual = df_anual[[
        'ano', 
        'decada', 
        'meta_acumulada_ano', 
        'selic_acumulada_ano', 
        'ipca_acumulado_ano'
    ]]
    
    filepaths = {
        'mes': os.path.join(DATA_PATH, 'selic_ipca_mes.tsv'), 
        'ano' : os.path.join(DATA_PATH, 'selic_ipca_ano.tsv'),
    }

    for df, filepath in zip((df_mensal, df_anual), filepaths): save_to_csv(df, filepath, '\t')

    return filepaths


if __name__ == '__main__':
    run()
