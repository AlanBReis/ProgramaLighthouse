from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import shutil
import psycopg2
import csv

def extrair_csv():
    origem = '/home/alan/Documentos/Programms/ProgramaLighthouse/arquivos/transacoes.csv'
    data = datetime.now().strftime('%Y-%m-%d')
    destino = f'/home/alan/Documentos/Programms/ProgramaLighthouse/Datalake/{data}/csv'
    
    if not os.path.exists(destino):
        os.makedirs(destino)

    shutil.copy(origem, destino + '/transacoes.csv')
    print('Arquivo CSV copiado para:', destino)


def extrair_sql():
    data = datetime.now().strftime('%Y-%m-%d')
    destino = f'/home/alan/Documentos/Programms/ProgramaLighthouse/Datalake/{data}/sql'
    
    if not os.path.exists(destino):
        os.makedirs(destino)

    try:
        conexao = psycopg2.connect(
            dbname='banvic',
            user='data_engineer',
            password='v3rysecur&pas5w0rd',
            host='localhost',
            port='55432'
        )
        cursor = conexao.cursor()
        cursor.execute('SELECT * FROM clientes')
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]

        with open(destino + '/clientes.csv', 'w', newline='', encoding='utf-8') as arquivo:
            escritor = csv.writer(arquivo)
            escritor.writerow(colunas)
            escritor.writerows(dados)

        print('Arquivo SQL exportado para:', destino)

        cursor.close()
        conexao.close()
    except Exception as erro:
        print('Erro ao extrair dados do SQL:', erro)

def carregar_datawarehouse():
    data = datetime.now().strftime('%Y-%m-%d')
    caminho_transacoes = f'/home/alan/Documentos/Programms/ProgramaLighthouse/Datalake/{data}/csv/transacoes.csv'
    caminho_clientes = f'/home/alan/Documentos/Programms/ProgramaLighthouse/Datalake/{data}/sql/clientes.csv'

    try:
        conexao = psycopg2.connect(
            dbname='seu_datawarehouse',      # nome do banco DW no container dw
            user='usuario_dw',               # usuário DW
            password='senha_dw',             # senha DW
            host='localhost',
            port='5433' 
        )
        cursor = conexao.cursor()

        # Cria a tabela transacoes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transacoes (
            cod_transacao INTEGER,
            num_conta INTEGER,
            data_transacao TIMESTAMP,
            nome_transacao TEXT,
            valor_transacao NUMERIC
        );
        """)

        # Cria a tabela clientes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
            id SERIAL PRIMARY KEY,
            nome TEXT,
            email TEXT,
            data_nascimento DATE
        );
        """)

        conexao.commit()

        # Carrega dados do arquivo CSV para a tabela transacoes
        if os.path.exists(caminho_transacoes):
            with open(caminho_transacoes, 'r') as f:
                # Pula o cabeçalho do arquivo
                next(f)  
                cursor.copy_from(f,
                                 'transacoes',
                                 sep=',',
                                 columns=('cod_transacao', 
                                          'num_conta', 
                                          'data_transacao', 
                                          'nome_transacao', 
                                          'valor_transacao'))
            print(f"Dados de transacoes carregados com sucesso!")
        else:
            print(f'Arquivo {caminho_transacoes} não encontrado.')

        # Carrega dados do arquivo CSV para a tabela clientes
        if os.path.exists(caminho_clientes):
            with open(caminho_clientes, 'r') as f:
                # Pula o cabeçalho do arquivo
                next(f)
                cursor.copy_from(f, 
                                 'clientes', 
                                 sep=',', 
                                 columns=('nome', 
                                          'email', 
                                          'data_nascimento'))
            print(f"Dados de clientes carregados com sucesso!")
        else:
            print(f'Arquivo {caminho_clientes} não encontrado.')

        conexao.commit()
        cursor.close()
        conexao.close()

        print("Carregamento no Data Warehouse finalizado com sucesso!")

    except Exception as e:
        print("Erro no carregamento do Data Warehouse:", e)

with DAG(
    'etl_desafio_lighthouse',
    start_date=datetime(2025, 9, 3),
    schedule_interval='35 4 * * *',
    catchup=False
) as dag:

    extrair_csv_task = PythonOperator(
        task_id='extrair_arquivo_csv',
        python_callable=extrair_csv
    )

    extrair_sql_task = PythonOperator(
        task_id='extrair_arquivo_sql',
        python_callable=extrair_sql
    )

    datalake = EmptyOperator(
        task_id='datalake', 
        trigger_rule='all_success'
    )

    carregar_dw = PythonOperator(
        task_id='carregar_datawarehouse',
        python_callable=carregar_datawarehouse
    )

    
    [extrair_csv_task, extrair_sql_task] >> datalake >> carregar_dw