
import os
from modules import DataManager, DataConn, DbMAnager
from modules.Utilities import send_email
from dotenv import load_dotenv
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

### El archivo __main__ ejecutara todo nuestro código desarrollado en el archivo ETL_manager,
### asignando los valores necesarios para la ejecución de cada método.

#Llamar al método de la libreria dotenv para obtener nustras variables de entorno 
load_dotenv()

# Agregar a un diccionario nuestras credenciales 
user_credentials = {
    "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
    "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
    "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
    "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
    "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
}

# Asignar las variables table y schema, con el nombre de la tabla y esquema correspondiente
table='stage_spotify_new_releases_table'
schema = "mauroalberelli_coderhouse"

# Instanciar las Clases
db_manager = DbMAnager(user_credentials, schema)
SpotifyApi = DataManager()

# Definir nuestras funciones para pasarle al operador del DAG()
def start_conn(): 
    db_manager.connect_Db()

def tables():
    db_manager.create_table(table)  

def get_and_transform():
    data=SpotifyApi.data_transform()
    return data

def insert_data(data,table):
    db_manager.upload_data(data,table)

def get_sql_result(**kargs):
    ti=kargs['ti']
    db_manager.get_query_result(ti,table)
    

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Mauro',
    'start_date': datetime(2024,6,29),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
    
}

# Definir el DAG con intervalo @daily(diario) para su ejecución
with DAG(dag_id='Spotify_data_pipeline',
        default_args=default_args,
        description='Agrega datos de los 50 ultimos lanzamientos en spotify',
        schedule_interval="@weekly",
        catchup=False) as dag:
    
    # Pasar nuestras funciones creadas para que las ejecute el Operator
    task_conexion = PythonOperator(task_id='Conect-Redshift',python_callable=start_conn)
    task_tables = PythonOperator(task_id='Create-Tables',python_callable=tables)
    # Pasar como argumentos funciones en un lambda para recuperar y trasmitir de tarea a tarea los resultados de dichas funciones
    task_process_data = PythonOperator(task_id='Process-Data',python_callable=lambda:get_and_transform())
    task_insert_data = PythonOperator(task_id='Upload-Data',python_callable=lambda:insert_data(get_and_transform(),table))
    task_get_sql_result = PythonOperator(task_id='Get_sql_result',python_callable=get_sql_result,provide_context=True)
    task_send_email = PythonOperator(task_id='Send_email',python_callable=send_email,provide_context=True)

# Establecer el orden de ejecución de nuestras tareas
task_conexion >> task_tables >> task_process_data >> task_insert_data >> task_get_sql_result >>  task_send_email