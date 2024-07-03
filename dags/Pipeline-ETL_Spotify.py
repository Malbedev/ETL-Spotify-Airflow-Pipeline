
import os
from modules import DataManager, DataConn
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
data_conn = DataConn(user_credentials, schema)
SpotifyApi = DataManager()

# Definir nuestras funciones para pasarle al operador del DAG()
def start_conn(): 
    data_conn.connect_Db()
def tables():
    data_conn.create_table(table)  
def get_and_transform():
    data=SpotifyApi.data_transform()
    return data
def insert_data(data,table):
    data_conn.upload_data(data,table)

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
        schedule_interval="@daily",
        catchup=False) as dag:
    
    # Pasar nuestras funciones creadas para que las ejecute el Operator
    task_conexion = PythonOperator(task_id='conexion',python_callable=start_conn)
    task_tables = PythonOperator(task_id='tablas',python_callable=tables)
    # Pasar como argumentos funciones en un lambda para recuperar y trasmitir de tarea a tarea los resultados de dichas funciones
    task_process_data = PythonOperator(task_id='data',python_callable=lambda:get_and_transform())
    task_insert_data =PythonOperator(task_id='upload',python_callable=lambda:insert_data(get_and_transform(),table))

# Establecer el orden de ejecución de nuestras tareas
task_conexion >> task_tables >> task_process_data >> task_insert_data