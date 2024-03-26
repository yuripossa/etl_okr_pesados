import psycopg2
import pandas as pd
from io import BytesIO
#from Auxiliares_OKR_Pesados.passwords import SHAREPOINT_PASSWORD
#from Auxiliares_OKR_Pesados.variables import SITE_URL, SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD
from io import StringIO
import pymssql

#region Conexão com bancos
def get_data_from_db(db_type, username, password, dbname, host, port, query):    
    if db_type == "Mssql":
        hook = pymssql.connect(server=host, user=username, password=password, database=dbname, port=port)
        data = pd.read_sql_query(query, hook)
    if db_type == "Postgres":
        hook = psycopg2.connect(host=host, port=port, database=dbname, user=username, password=password)
        data = pd.read_sql_query(query, hook)
    return data
#endregion

#region Inserte de Dados no Postgress
def PostgreSQL_Insert(username, password, dbname, host, data, table):
    conn = psycopg2.connect(dbname=dbname, user=username, password=password, host=host)
    cur = conn.cursor()
    # Criar um buffer de string para escrever os dados como CSV
    sio = StringIO()
    data.to_csv(sio, index=None, header=None)
    sio.seek(0)
    # Ler os dados do buffer de string e inserir em lote no PostgreSQL
    cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", sio)
    # Commit das alterações
    conn.commit()
    # Feche o cursor e a conexão
    cur.close()
    conn.close()
    return data
#endregion

def PSSql_Delete(db_type, username, password, dbname, host, port, query):
    conn = psycopg2.connect(host=host, user=username, password=password, dbname=dbname, port=port)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()