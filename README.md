# Analise-Dados-Macroeconomicos

O intuito de desse repositório é abrigar uma pipeline de dados Macroeconômicos bbrasileiros além da ánalise que fiz. Um dos objetivos é utilizar Airflow para coordenar os fluxos de dados e agendamentos. Paralelamente a isso, eu fiz uma análise dos indicadores utilizando os dados que foram resultados do processo de ETL. 

No notebook Analise-Selic-IPCA-pt1.ipynb, há todo o processo de elaboração da ETL ajustado as necessidades para elaboração da análise (perguntas e resposta) e suas justificativas.

Como, do modo que eu concebi a extração de dados, foi necessário a utilização do selenium e dado que meu intuito é fazer com que o script de scrap sempre funcione, optei por isolar esse processo para garantir replicabilidade. 

Para obter os dados mais recentes da selic basta usar o seguinte comando

'''bash
docker build -t scrap -f Dockerfile_v2 .
echo -e $(docker run scrap)
'''

Minha opção por retornar os dados utilizando o print foi muito mais voltada a velocidade e também pela quantidade de dados não ser muito volummosa.

No notebook Analise-Selic-IPCA-pt2.ipynb eu utilizo os dados já transformados para poder fazer uma análise exploratória (para não só compreender os dados como trazer insights) e estatística dos dados e uma tentative de modelar os dados.



