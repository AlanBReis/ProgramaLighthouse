Desafio de Engenharia de Dados - BanVic

O que é este projeto?

Este é um projeto de engenharia de dados que constrói um pipeline de ETL (Extração, Transformação e Carga) para um banco fictício. O objetivo é extrair dados de clientes e transações de fontes distintas (um banco de dados e um arquivo CSV) e carregá-los em um Data Warehouse. A automação e orquestração do processo são feitas com o Apache Airflow.

Tecnologias

    Apache Airflow

    PostgreSQL

    Python

    Docker

Como Executar

    Tenha o Docker e o Docker Compose instalados.

    Abra o terminal na pasta do projeto.

    Inicie os serviços do Docker com o comando:
    Bash

docker-compose up -d

Acesse a interface do Airflow em http://localhost:8080.

Ative a DAG etl_desafio_lighthouse e execute-a para iniciar o pipeline.


Resultados (Com Evidências)

1. DAG em Execução (Airflow UI)

    Status de Sucesso: Uma imagem da DAG mostrando todas as tarefas executadas com sucesso.

    Logs de Execução: Uma imagem dos logs da tarefa carregar_datawarehouse, mostrando as mensagens de sucesso.

2. Estrutura de Arquivos Criada (Filesystem Local)

    Uma imagem da estrutura de diretórios criada no caminho /Datalake após a execução do pipeline.

3. Dados Carregados no Data Warehouse (psql ou GUI)

    Verificação por Terminal (psql): Uma imagem do terminal mostrando a conexão ao Data Warehouse e a contagem de linhas nas tabelas transacoes e clientes.

    