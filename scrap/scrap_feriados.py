from __future__ import annotations
import requests
import pandas as pd
import numpy as np
import holidays
from bs4 import BeautifulSoup

def scrap_feriados() -> pd.DataFrame:
    url = 'https://www.inf.ufrgs.br/~cabral/tabela_pascoa.html'
    feriados_data = requests.get(url).text
    feriados_soup = BeautifulSoup(feriados_data, 'html5lib')
    feriados_tabela = feriados_soup.find_all("table")
    feriados_df = pd.read_html(str(feriados_tabela))[0]
    return feriados_df

def feriados_transform(df: pd.DataFrame) -> pd.DataFrame:
    MESES_NUM = {
        'jan':'1', 
        'fev':'2', 
        'mar':'3', 
        'abr':'4', 
        'mai':'5', 
        'jun':'6', 
        'jul':'7', 
        'ago':'8', 
        'set':'9', 
        'out':'10', 
        'nov':'11', 
        'dez':'12'
    }

    #transform the dataframe
    feriados_df = df
    feriados_df = feriados_df.iloc[:, -3:]
    feriados_df.columns = feriados_df.iloc[0, :]
    feriados_df = feriados_df.iloc[1:, :].reset_index(drop = True)
    
    #replacing months string for number and formatting 
    for feriado_col in feriados_df:
        feriados_df[feriado_col] = feriados_df[feriado_col]\
                .replace(MESES_NUM, regex=True)
        feriados_df[feriado_col] = pd.to_datetime(
                feriados_df[feriado_col], format="%d/%m/%Y"
        )
    #adding another holiday, day preceding brazillian carnival
    feriados_df['Pré Carnaval'] = feriados_df['Carnaval'] +\
            pd.Timedelta(days = -1)

    return feriados_df

def main():
    #getting holidays dict from holidays library
    feriados_dict = holidays.Brazil(years = range(1951,2079))
    dict_holi = dict(
            (val, [key for key in feriados_dict.keys() if val in feriados_dict[key]]) for val in feriados_dict.values()
    )

    #some holidays have fewer rows, so we retrieve them as a variable
    feriado_cn = dict_holi['Dia Nacional de Zumbi e da Consciência Negra']
    feriado_nsa = dict_holi['Nossa Senhora Aparecida']

    #del keys with low number of values
    del dict_holi['Sexta-feira Santa; Tiradentes']
    del dict_holi['Dia Nacional de Zumbi e da Consciência Negra']
    del dict_holi['Nossa Senhora Aparecida']

    #create a df
    feriados_df = pd.DataFrame(dict_holi)

    #add the uneven 
    feriados_df['Consciência Negra'] = pd.Series(feriado_cn)\
            .reindex(np.arange(0, len(feriados_df)), fill_value = pd.NaT)
    feriados_df['Nossa Senhora Aparecida'] = pd.Series(feriado_nsa)\
            .reindex(np.arange(0, len(feriados_df)), fill_value = pd.NaT)
 
    #retrieve via scrap hodidays not present in the holidays library
    feriados_scrap_df = scrap_feriados()
    feriados_scrap_df = feriados_transform(feriados_scrap_df)

    #merge
    feriados_df = pd.merge(
            left = feriados_df, 
            right = feriados_scrap_df, 
            left_index = True, 
            right_index = True
    )

    feriados_df.to_csv('../data/feriados.csv', index = False)

if __name__ == '__main__':
    main()
    
