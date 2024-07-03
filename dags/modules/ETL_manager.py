
import os
from modules.Utilities import  hashing_data
import pandas as pd
import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Llamar al método de la libreria dotenv para obtener nuestras variables de entorno 
load_dotenv()

# Intanciar las claves de Spotify con nuestras variables de entorno
CLIENT_ID =os.getenv('CLIENT_ID')
CLIENT_SECRET =os.getenv('CLIENT_SECRET')

# Generer un logger para manejo de erroes e info de proccesos
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

## Gererar la clase constructura para la conexión a la base de datos
# que admite como parametros un diccionario con las credenciales y un STring con el nombnre del schema ## 

class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.table = None
        self.username = self.config.get('REDSHIFT_USERNAME')
        self.password = self.config.get('REDSHIFT_PASSWORD')
        self.host = self.config.get('REDSHIFT_HOST')
        self.port = self.config.get('REDSHIFT_PORT', '5439')
        self.dbname = self.config.get('REDSHIFT_DBNAME')

    # Conectar la base da datos con el módulo de psycopg2 
    def connect_Db(self):
        try:
            psycopg2.connect(host=self.host,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
            port=self.port
        )
            logging.info("Connection created")

        except Exception as e:
            logging.error(f"Failed to create connection:{e}")
            raise

    # Crear una tabla con lenguaje SQL atravéz de la librería psycopg2 , recibe un string con el nombre de la tabla
    def create_table(self,table:str):
            logging.info("Creating table....")
            schema=self.schema
            try: 
                conn = psycopg2.connect(host=self.host,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
            port=self.port
        )
                
                with conn.cursor() as cur:
                 cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}
                (
                id VARCHAR(50) primary key  
                ,Album_type VARCHAR(50)   
                ,Album_name VARCHAR(50)  
                ,Artist_name VARCHAR(50)      
                ,Total_tracks INTEGER  
                ,Album_genre VARCHAR(500)
                ,Realese_date date   
                ,Album_img VARCHAR(300)
                ,Album_link VARCHAR(300)
                ,Artist_link VARCHAR(300)
                ,Load_date date 
                )
            """)
                conn.commit()
                logging.info(f" Table created! ")
     
            except Exception as e:
                logging.error(f"Failed to create table:{e}")
                raise

    # Cargar la data en nuestra tabla creada anteriormente
    ### Función que recibe com parámetros un Dataframe de pandas y un string con el nombre de la tabla ###
    def upload_data(self,data: pd.DataFrame, table: str):
         
         logging.info("inserting data to table....")
         conn = psycopg2.connect(host=self.host,
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                port=self.port
                )
         df = pd.DataFrame(data)

         with conn.cursor() as cur:
                try:
                    cur.execute(f"Truncate table {table}") 
                    conn.commit()

                    execute_values(
                    cur,
                f'''
                    INSERT INTO {table} (Id, Album_type, Album_name, Artist_name, Total_tracks, 
                    Album_genre, Realese_date, Album_img, Album_link, Artist_link,
                    Load_date )
                    VALUES %s
                    ''',
                    [tuple(row) for row in df.to_numpy()],
                    page_size=len(df)
                )
                    conn.commit()
                    logging.info(f"Data from the DataFrame has been uploaded to the {self.schema}.{table} table in Redshift.")
                    conn.close()
                    logging.info("Connection to Redshift closed.")
                except Exception as e:
                    logging.error(f"Failed to upload data to {self.schema}.{table}: {e}")
                    
        
## Crear una clase para el manejo de los datos ## 
class DataManager:

    def __init__(self):
        self.data = None

    # Extraer la data de la Api

    ### Esta función nos devuelve los datos obtenidos de la api  
    # en una lista de diccionarios,con los valores requeridos ###
    def data_extract(self):
       
        try:
            logging.info("Extracting data from Api ...")
            client_id =CLIENT_ID
            client_secret =CLIENT_SECRET
            client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
            # Extraer los datos con la libreria Spotipy
            sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            results = sp.new_releases(limit=50)

            # Crear la lista destino donde se alojaran los datos con las columnas que necesitamos
            data = {'Id': [],'Album_type': [], 'Album_name': [],'Artist_name': [],'Total_tracks': [],'Album_genre':[], 'Realese_date': [], 'Album_img': [],'Album_link':[],'Artist_link':[],'Load_date': []}
            # Iterar los elementos necesarios e instanciamos cada uno de ellos
            for album in results['albums']['items']:
                id = album['id']
                album_type = album['album_type']
                album_name = album['name']
                artist_name = album['artists'][0]['name']
                total_tracks = album['total_tracks']
                artist_id = album['artists'][0]['id']
                album_genre = sp.artist(artist_id)['genres']
                realese_date = album['release_date']
                album_img = album['images'][0]['url'] 
                album_link = album['external_urls']['spotify']
                artist_link = album['artists'][0]['external_urls']['spotify']

                # Quitar las comillas 
                album_name = album_name.replace("'", "")
                # Separar el género por coma
                album_genre = ', '.join(album_genre)
                # Agregamos una fecha de carga de datos 
                now = datetime.date.today().strftime('%Y-%m-%d')

                # Agregar los valores obtenidos a la lista creada anteriormente, según corresponda
                data['Id'].append(id)
                data['Album_type'].append(album_type)
                data['Album_name'].append(album_name)
                data['Artist_name'].append(artist_name)
                data['Total_tracks'].append(total_tracks)
                data['Album_genre'].append(album_genre)
                data['Realese_date'].append(realese_date)
                data['Album_img'].append(album_img)
                data['Album_link'].append(album_link)
                data['Artist_link'].append(artist_link)
                data['Load_date'].append(now)

            return data
                  
        except Exception as e:
            logging.error(e)
        finally:
            logging.warn("Check the data format")

    
    # Trasformar la data con pandas

    ### Esta función nos devuelve un Dataframe de pandas con las trasformaciones correpondientes###
    def data_transform(self):
        try:
            # Llamar a nuestra función de extracción e instanciar en una variable
            data = self.data_extract()
            logging.info('Data to Pandas Dataframe')
            # Instaciar una nueva variable con los datos obtenidos en un data frame de Pandas
            df = pd.DataFrame(data)

            # Limpiar los datos

            # Evitar que haya duplicados
            df.drop_duplicates(subset=['Album_name', 'Artist_name'], keep='first', inplace=True)
            # Reemplazar valores nulos o vacios en el campo Género por Desconocido
            df.fillna({'Album_genre':'Desconocido'}, inplace=True)
            df.loc[df['Album_genre'] == '', 'Album_genre'] = 'Desconocido'
            # Verificar que la fecha se muestre en formato fecha 
            df['Realese_date'] = pd.to_datetime(df['Realese_date'], format='%Y-%m-%d')
            df['Realese_date'] = df['Realese_date'].dt.strftime('%Y-%m-%d')

            # Hashear información utilizando nuestra función hashing_data del módulo Utilities
            # Como ejemplo lo hacemos con el Id del albúm
            df['Id'] = df['Id'].apply(hashing_data)

            return(df)
               
        except Exception as e:
            logging.error(e)

