from Auxiliares_OKR_Pesados.variables import(DEALER_USERNAME,DEALER_PASSWORD,DEALER_DBNAME,DEALER_HOST,DEALER_PORT,date_start,SCHEMA_CRIACAO_INSERT,DW_CORPORATIVO_USERNAME,
DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT)
from Auxiliares_OKR_Pesados.querys import (query_cpc47, query_okr_pesados, deleteOkrPesados,deleteCpc47,deleteRelOKRPesados,consultaRelatorioOKR)
from Auxiliares_OKR_Pesados.my_functions_OKR import (get_data_from_db,PostgreSQL_Insert,PSSql_Delete)
import pytz
import pandas as pd
from datetime import datetime, timedelta
#import pymssql 
from airflow import DAG
from airflow.operators.python import PythonOperator

#region Conexão OKR Pesados
def Query_OKR_Pesados():
    try:
        data = get_data_from_db("Mssql", DEALER_USERNAME, DEALER_PASSWORD, DEALER_DBNAME, DEALER_HOST, DEALER_PORT, query_okr_pesados)        
       
    except Exception as e:
        print("Não foi possivel acessar o banco.")
        data = None
    return data

#endregion

#region Conexão CPC47
def Query_CPC47():
    try:
        data = get_data_from_db("Mssql", DEALER_USERNAME, DEALER_PASSWORD, DEALER_DBNAME, DEALER_HOST, DEALER_PORT, query_cpc47)       
        
    except Exception as e:
        print("Não foi possivel acessar o banco.")
        data = None
    return data

#endregion

#region Deletes
def delete_OKR_Pesados():
    PSSql_Delete("Postgres",DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT,deleteOkrPesados)

def delete_CPC47():
    PSSql_Delete("Postgres",DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT,deleteCpc47)
#endregion

#region Inserts
def Insert_OKR_Pesados():
    dataokr = Query_OKR_Pesados()
    PostgreSQL_Insert(DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,dataokr,'staging.dim_okr_pesados')

def Insert_CPC47():
    dataCPC47 = Query_CPC47()
    PostgreSQL_Insert(DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,dataCPC47,'staging.dim_okr_cpc47_pesados')

#endregion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# Configurando o fuso horÃƒÂ¡rio para SÃƒÂ£o Paulo
sp_timezone = pytz.timezone('America/Sao_Paulo')

# Definindo a data e hora desejada em SÃƒÂ£o Paulo
start_date = sp_timezone.localize(datetime(2024, 3, 6, 7, 45))

with DAG(
    'ETL_OKR_PESADOS',
    default_args=default_args,
    description='DAG para ETL de dados OKR de Pesados',
    schedule_interval='45 0,11,17 * * *',
    start_date=start_date,
    catchup=False,
) as dag:
        okrpesados = PythonOperator(task_id = "OKR_PESADOS",python_callable=Query_OKR_Pesados)
        cpc47 = PythonOperator(task_id = "CPC47",python_callable=Query_CPC47)
        #relOkr = PythonOperator(task_id = "REL_OKR",python_callable=Query_Rel_OKR)
        insertokr = PythonOperator(task_id = "INSERT_OKR_PESADOS",python_callable=Insert_OKR_Pesados)
        insertCPC47 = PythonOperator(task_id = "INSERT_CPC47",python_callable=Insert_CPC47)
        #insertrelokr = PythonOperator(task_id = "INSERT_REL_OKR",python_callable=Insert_Rel_OKR)
        deleteokr = PythonOperator(task_id = "DELETE_OKR_PESADOS",python_callable=delete_OKR_Pesados)
        deletecpc47 = PythonOperator(task_id = "DELETE_CPC47",python_callable=delete_CPC47)
        #deleterelokr = PythonOperator(task_id = "DELETE_REL_OKR",python_callable=delete_rel_OKR)
    
 # Estrutura das tarefas
[deleteokr,deletecpc47] >> okrpesados >> insertokr >> cpc47 >> insertCPC47
# [deleteokr,deletecpc47] >> [okrpesados,cpc47] >> [insertokr,insertCPC47]