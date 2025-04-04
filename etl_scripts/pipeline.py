import pandas as pd
import numpy as np

def main():
    def read_selic(path) -> pd.DataFrame:
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

    def read_ipca(path) -> pd.DataFrame:
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
            names = IPCA_COLS) 

        ipca_df = ipca_df[ipca_df.isnull().sum(axis = 'columns') <= 1]\
                .reset_index(drop=True)

        return ipca_df

    def transform_selic(selic_df: pd.DataFrame) -> pd.DataFrame:
        #read feriados_file
        feriados_df = pd.read_csv('dados/feriados.csv')

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

        #resampling
        resampledselic_df = selic_df[['vigencia_inicio', 'taxa_selic_pctaa', 'meta_selic_pctaa']]\
                .set_index('vigencia_inicio')\
                .apply(lambda x: np.power(1 + x/100, 1/252))\
                .resample('B')\
                .ffill()
        
        return resampledselic_df

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
        
        #month to numberic
        ipca_df['mes_num'] = ipca_df['mes'].map(meses_numcap)

        #formating for type PeriodIndex
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
        ipca_df = ipca_df[['mes', 'periodo_mes', 'ano', 'ipca_no_ano', 'ipca_var_mensal']]

        #create anew column: decada
        ipca_df['decada'] = (np.floor(ipca_df['ano'] / 10) * 10).astype('int')

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

    def save_to_csv(df: pd.DataFrame, path: str):
        df.to_csv('dados/selic_ipca_noano.tsv', sep = '\t', index = False)

if __name__ == '__main__':
    main()
