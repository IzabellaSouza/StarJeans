![alt text](https://github.com/IzabellaSouza/StarJeans/blob/ee9f05b87d0547dcccaec83166cb82ac12732bdb/img/STARJEANS.png)

# O Problema de negócio

Eduardo e Marcelo são dois brasileiros, amigos e sócios de empreendimento. Depois de vários negócios bem sucedidos, eles estão planejando entrar no mercado de moda dos USA como um modelo de negócio do tipo E-commerce.

A ideia inicial é entrar no mercado com apenas um produto e para um público específico, no caso o produto seria calças Jeans para o público masculino. O objetivo é manter o custo de operação baixo e escalar a medida que forem conseguindo clientes. Porém, mesmo com o produto de entrada e a audiência definidos, os dois sócios não tem experiência nesse mercado de moda e portanto não sabem definir coisas básicas como preço, o tipo de calça e o material para a fabricação de cada peça.

Assim, os dois sócios contrataram uma consultoria de Ciência de Dados para responder as seguintes perguntas:

1.	Qual o melhor preço de venda para as calças?
2.	Quantos tipos de calças e suas cores para o produto inicial?
3.	Quais as matérias-primas necessárias para confeccionar as calças?

As principais concorrentes da empresa Start Jeans são as americanas H&M e Macys.

# Planejamento da Solução - O método SAPE

## Problema de Negócio

1.	Qual o melhor preço de venda para as calças? 
2.	Quantos tipos de calças e suas cores para o produto inicial? 
3.	Quais as matérias-primas necessárias para confeccionar as calças? 

## Saída (O produto final) 

- Resposta para a pergunta
   - A mediana dos valores dos produtos do site dos concorrentes. 
- Formato
   - Tabela ou gráfico 
- Local de entrega 
   - App no Streamlit 

## Processo ( Passo a passo ) 

- Passo a passo para calcular a resposta?
  - Mediana do preço por categoria e tipo. 
- Como será o gráfico ou tabela final?
  - Simulação da tabela final
- Como será o local de entrega?
  - Dashboard em um app no Streamlit 


## Entradas (Fontes de dados)

H&M: https://www2.hm.com/en_us/men/products/jeans.html

## Ferramentas

- Python 3.8.0
- Bibliotecas de Webscrapping (BS4, Selenium)
- PyCharm
- Jupyter Notebook (Analise e prototipagens)
- Crontjob, Airflow
- Streamlit

# ETL

- Extração de dados via Webscraping com a biblioteca de Python Beautiful Soup
- Limpeza dos dados
- Armazenamento dos dados
- Tabela em um banco de dados
- ETL completo
- Transformando Notebook em Script Python
 
