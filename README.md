# Analise-Dados-Macroeconomicos </br> (Data Analysis of Brazillian Interest Rate and Inflation Rate)

## Project Description

This project aims to study the Brazillian interest rate and inflation rate leveraging the historic-to-current data 
that is available in the internet. 
This repository contains a jupyter notebook detailing how I did concept the ETL process and a second jupyter notebook with
data analysis of the transformed data. The ETL process is now orchestrated using Apache Airflow.

## Project summary

This project started as a way for me to apply some statistical concepts I was studying at the time, primarily 
related to time series. At that time, I kept hearing a lot about how interest rates were constantly increasing, 
so I decided to try to understand it better. What I discovered was that interest rates were somehow related to the 
inflation rate, which led me to start studing it as well.

Since I wanted the studying to be mainly driven by the data, as this is a data science project, my first move was 
to find where to get all the data that I need. While searching the internet I came across a table hosted on Brazil's 
Central Bank website that contained the historical-to-current interest rate data, but I had no clue how would I retrieve it. 
I was only able to extract it using selenium, as the table was dynamically rendered. For the inflation rate, it was easier at first 
but after reading it with pandas, the data ended up being poorly formated, but I managed to come up with a  clever solution to 
make it usable.

The next step was data cleaning, so I began checking if each piece of information matched what was available on the web. 
To my surprise, the interest rate's table had annual and monthly interest rates mixed. It also had date inconsistencies that 
needed to be corrected before the data could be used. 

The end goal in the processing phase was to merge both the interest rate and the inflation rate, but while the inflation rate is 
monthly, the inflation rate had uneven periods, which made the merge not possible. The next task was to turn selic into a monthly 
basis which was done following the 252 working days.

## Project Structure

```
.
├── Analise-Selic-IPCA-pt1.ipynb
├── Analise-Selic-IPCA-pt2.ipynb
├── Dockerfile
├── ETL_planning.ipynb
├── Interest_Inflation_rates_data analys.b
├── README.md
├── dados
│   ├── feriados.csv
│   ├── selic.tsv
│   ├── selic_ipca_ano.tsv
│   ├── selic_ipca_mes.tsv
│   └── selic_transformacao_preliminar.tsv
├── dags
│   └── etl_dag.py
├── etl-airflow.png
├── etl_scripts
│   ├── __init__.py
│   └── pipeline.py
├── requirements_airflow.txt
├── scrap
│   ├── scrap_feriados.py
│   └── selic_scrapper
│       ├── requirements.txt
│       └── selic_scrapper.py
├── setup.py
└── setup_airflow.sh
```

## Schemas

### Schema of the extracted data 
#### Interest Rate (Selic)
| Variável <br/> (Variable) | Descrição <br/> (Description)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|---------------------------|-------------------------------|
| reuniao_num               | Classificação ordinal da reunião do Copom <br/>(Ordinal classification of the Copom's meeting) |
| reuniao_data              | Data da reunião <br/>(Copom meeting's date)  |
| reuniao_vies              | Indicativo de tendência de mudança da taxa Selic. Essa mudança pode ser feita na meta, na direção do viés, para a taxa Selic a qualquer momento entre as reuniões ordinárias. <br/> (The indicated bias for the upcoming change in the target interest rate. This change may be implemented in accordance with the bias, at any time.) |
| periodo_vigencia          | Periodo em que a meta selic fica vigente.<br/> (The time period which the target interest rate is /was in place.) |                                  
| meta_selic_pctaa          | Meta de juros (anual) como referência.<br/> (The established annual interest rate, set as a reference) |
| tban_pctam                | Taxa de Assistência do Banco Central: é uma taxa cobrada em empréstimos quando bancos não possuem títulos públicos para oferecer como garantia, ou quando superam os limites de crédito da linha que utiliza a Taxa Básica do banco central. A TBAN foi criada em 28/8/96 e extinta em 4/3/99. (The Brazillian Central Bank Assistance Rate is an instrument that is charged on loans when banks do not have government bonds to offer as collateral or when they exceed the credit limits of the line that uses the Central Bank's Basic Rate.)     |
| taxa_selic_pct            | Taxa média ponderada e ajustada dos financiamentos diários apurados no Sistema Especial de Liquidação e de Custódia (Selic) para operações compromissadas de um dia (overnight) lastreadas em títulos públicos federais, acumulada no período. Títulos públicos são títulos emitidos pelo governo federal e são utilizados por ele para se financiar. <br/> (The weighted and adjusted average of the daily financing transactions calculated by the SELIC (a Special Settlement and Custody System) to the one-day repurchase operations backed by government bonds and accumulated over the period. |
| taxa_selic_pctaa          | Taxa selic anualizada com base em 252 dias úteis. <br/>(The annual interest rate based on 252 working days) |

#### Inflation Rate (IPCA)

| Variável (Variable)   | Descrição (Description)                                                                                                                                                                                                                            
|-----------------------|----------------------------------------------------------------------------------|
| ano                   | Ano numérico <br/> (4-digit numeric Year)                                          |
| mes                   | Nome do mês limitado a três letras (Name of the month limited to three letters)    |
| ipca_numero_indice    | Média aritmética ponderada dos 16 índices metropolitanos mensais, que são calculados pela fórmula de Laspeyres. <br/>(Weighted arithmetic average of the 16 monthly average Brazillian metropolitan indeces, computed using the Laspeyres Formula) |        | ipca_var_mensal       | Variação mensal do índice durante o mês. <br/> (Monthly variation of the IPCA index over a month)  |             | ipca_var_trimestral   | Variação trimestral do índice considerando os últimos 3 meses. <br/> (Quarterly variation of the index considering the last 3 months)   |                                          
| ipca_var_semetral     | Variação semestral do índice considerando os últimos 6 meses. <br/> (Six-month change in the index over the last 6 months.)    |         
| ipca_no_ano           | Variação do índice no mês referência em relação ao índice de dezembro do ano passado ao ano de referência.<br/> (IPCA index variation in the reference month compared to the index in December of the previous year, for the reference year.) |   
| ipca_acumulado_ano    | Soma da variação mensal de 12 meses. <br/> (Annual variation sum over 12 months )   | 


### Output Schemas

#### Monthly interest and inflation rates

| Variável <br/> (Variable) | Descrição  <br/>(Description)                                                             |
|---------------------------|-------------------------------------------------------------------------------------------|
| periodo-mes               | Período no formato YYYY-MM  <br/> (YYYY-MM format month)                                  |
| mes                       | Mês texto com três letras inicias <br/> (The first three letters of the respective month) |
|  ano                      | Ano <br/> (4-digit year)                                                                  |	
| decada                    | Década <br/> (4-digit decade)                                                             |
| meta_acumulada_mes        | Meta acumulada no mês <br/> (Monthly accumulated target interest rate.)                   | 
| selic_acumulada_mes       | Selic acumulada no mês <br/> (Monthly accumulated interest rate.)                         |
| ipca_mes                  | Inflação no mês  <br/> (Monthly Inflation rate.)                                          |


#### Annual interest and inflation rates

| Variável <br/> (Variable) | Decrição <br/> (Description)                                              |
|---------------------------|---------------------------------------------------------------------------|
| ano                       | Ano numérico com 4 digitos <br/>(4-digit numeric year)                    |
| decada                    | Década numérica com 4 digitos <br/> (4-digit numeric decade)              | 
| meta_selic_noano          | Meta Selic acumulada no ano<br/> (Annual accumulated target interest rate) |
| selic_acumulada_ano       | Selic acumulada no ano<br/> (Annual accumulated interest rate)            |
| ipca_acumulado_ano        | IPCA acumulado no ano <br/> (Annual accumulated inflation rate)           | 


## How to setup the pipeline using aiflow

![etl](etl-airflow.png)

You can start by creating a virtual environment using venv or virtualenv

```python
python3 -m venv econvenv
python3 econvenv/Scripts/activate
```

Then you can run setup_airflow.sh 

```python
bash setup_airflow.sh
```

Once the above script is finished, we can check the available dags
```python
airflow dags list
```

You can now create an user and run the airflow webserver or use the standalone version 
```python
airflow standalone
```

The standalone will generate an user and a password. 

