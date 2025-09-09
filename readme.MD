# Desafio de Engenharia de Dados - BanVic

## Sobre o Projeto

Este projeto de **Engenharia de Dados** constrói um **pipeline de ETL** (Extração, Transformação e Carga) para um banco fictício.  
O objetivo é extrair dados de clientes e transações de fontes distintas (um banco de dados e um arquivo CSV) e carregá-los em um **Data Warehouse**.  
A automação e orquestração do processo são realizadas utilizando o **Apache Airflow**.

---

## Tecnologias Utilizadas

- **Apache Airflow**  
- **PostgreSQL**  
- **Python**  
- **Docker**

---

## Como Executar

1. Certifique-se de ter o **Docker** e o **Docker Compose** instalados.
2. Abra o terminal na pasta do projeto.
3. Inicie os serviços do Docker com o comando:

```bash
docker-compose up -d
```
Acesse a interface do Airflow: http://localhost:8080

Ative a DAG etl_desafio_lighthouse e execute-a para iniciar o pipeline.

Resultados (Com Evidências)
1. DAG em Execução (Airflow UI)

Status de Sucesso:
Imagem da DAG mostrando todas as tarefas executadas com sucesso.

Logs de Execução:
Imagem dos logs da tarefa carregar_datawarehouse, mostrando mensagens de sucesso.

2. Estrutura de Arquivos Criada (Filesystem Local)

Imagem da estrutura de diretórios criada no caminho /Datalake após a execução do pipeline.

3. Dados Carregados no Data Warehouse (psql ou GUI)

Verificação por Terminal (psql):
Imagem do terminal mostrando a conexão ao Data Warehouse e a contagem de linhas nas tabelas transacoes e clientes.