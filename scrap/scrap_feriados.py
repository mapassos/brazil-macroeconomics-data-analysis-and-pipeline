import requests
import pandas as pd
from bs4 import BeautifulSoup

def scrap_feriados() -> pd.DataFrame:
    url = 'https://www.inf.ufrgs.br/~cabral/tabela_pascoa.html'
    feriados_data = requests.get(url).text
    feriados_soup = BeautifulSoup(feriados_data, 'html5lib')
    feriados_tabela = feriados_soup.find_all("table")
    feriados_df = pd.read_html(str(feriados_tabela))[0]
    return feriados_df

def main():
    
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

    feriados_df = scrap_feriados()
    feriados_df = feriados_df.iloc[:, -3:]
    feriados_df.columns = feriados_df.iloc[0, :]
    feriados_df = feriados_df.iloc[1:, :].reset_index(drop = True)
    
    feriados_cols = feriados_df.columns
    
    for feriado_col in feriados_cols:
        feriados_df[feriado_col] = feriados_df[feriado_col]\
                .replace(MESES_NUM, regex=True)
        feriados_df[feriado_col] = pd.to_datetime(
                feriados_df[feriado_col], format="%d/%m/%Y"
        )
    
    feriados_df['Pré Carnaval'] = feriados_df['Carnaval'] +\
            pd.Timedelta(days = -1)
    feriados_df.to_csv('../dados/feriados.csv', index = False)

if __name__ == '__main__':
    main()
    
